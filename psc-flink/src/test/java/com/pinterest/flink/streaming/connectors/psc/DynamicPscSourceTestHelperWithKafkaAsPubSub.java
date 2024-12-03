/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.dynamic.source.MetadataUpdateEvent;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.serde.Serializer;
import com.pinterest.psc.serde.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Brings up multiple kafka clusters and provides utilities to setup test data. */
public class DynamicPscSourceTestHelperWithKafkaAsPubSub extends PscTestBaseWithKafkaAsPubSub {
    public static final int NUM_PUBSUB_CLUSTERS = 2;

    public static void setup() throws Throwable {
        setNumKafkaClusters(NUM_PUBSUB_CLUSTERS);
        prepare();
    }

    public static void tearDown() throws Exception {
        shutDownServices();
    }

    public static ClusterTestEnvMetadata getKafkaClusterTestEnvMetadata(int kafkaClusterIdx) {
        return kafkaClusters.get(kafkaClusterIdx);
    }

    public static MetadataUpdateEvent getMetadataUpdateEvent(String topic) {
        return new MetadataUpdateEvent(Collections.singleton(getPscStream(topic)));
    }

    public static String getPubSubClusterId(int clusterIdx) {
        return kafkaClusters.get(clusterIdx).getKafkaClusterId();
    }

    /** Stream is a topic across multiple clusters. */
    public static PscStream getPscStream(String topic) {
        Map<String, ClusterMetadata> clusterMetadataMap = new HashMap<>();
        for (int i = 0; i < NUM_PUBSUB_CLUSTERS; i++) {
            ClusterTestEnvMetadata kafkaClusterTestEnvMetadata =
                    getKafkaClusterTestEnvMetadata(i);

            Set<String> topics = new HashSet<>();
            topics.add(topic);

            ClusterMetadata clusterMetadata =
                    new ClusterMetadata(
                            topics, kafkaClusterTestEnvMetadata.getStandardProperties());
            clusterMetadataMap.put(
                    kafkaClusterTestEnvMetadata.getKafkaClusterId(), clusterMetadata);
        }

        return new PscStream(topic, clusterMetadataMap);
    }

    public static void createTopic(String topic, int numPartitions, int replicationFactor) {
        for (int i = 0; i < NUM_PUBSUB_CLUSTERS; i++) {
            createTopic(i, topic, numPartitions, replicationFactor);
        }
    }

    public static void createTopic(String topic, int numPartitions) {
        createTopic(topic, numPartitions, 1);
    }

    public static void createTopic(int kafkaClusterIdx, String topic, int numPartitions) {
        createTopic(kafkaClusterIdx, topic, numPartitions, 1);
    }

    private static void createTopic(
            int kafkaClusterIdx, String topic, int numPartitions, int replicationFactor) {
        kafkaClusters
                .get(kafkaClusterIdx)
                .getPscTestEnvironmentWithKafkaAsPubSub()
                .createTestTopic(topic, numPartitions, replicationFactor);
    }

    /** Produces [0, numPartitions*numRecordsPerSplit) range of records to the specified topic. */
    public static List<PscProducerMessage<String, Integer>> produceToKafka(
            String topic, int numPartitions, int numRecordsPerSplit) throws Throwable {
        List<PscProducerMessage<String, Integer>> records = new ArrayList<>();

        int counter = 0;
        for (int kafkaClusterIdx = 0; kafkaClusterIdx < NUM_PUBSUB_CLUSTERS; kafkaClusterIdx++) {
            String kafkaClusterId = getPubSubClusterId(kafkaClusterIdx);
            List<PscProducerMessage<String, Integer>> recordsForCluster = new ArrayList<>();
            for (int part = 0; part < numPartitions; part++) {
                for (int i = 0; i < numRecordsPerSplit; i++) {
                    PscProducerMessage<String, Integer> message = new PscProducerMessage<>(
                            topic,
                            part,
                            topic + "-" + part,
                            counter++);
                    message.setHeader("flink.kafka-cluster-name", kafkaClusterId.getBytes(StandardCharsets.UTF_8));
                    recordsForCluster.add(message);
                }
            }

            produceToKafka(kafkaClusterIdx, recordsForCluster);
            records.addAll(recordsForCluster);
        }

        return records;
    }

    /**
     * Produces [recordValueStartingOffset, recordValueStartingOffset +
     * numPartitions*numRecordsPerSplit) range of records to the specified topic and cluster.
     */
    public static int produceToKafka(
            int kafkaClusterIdx,
            String topic,
            int numPartitions,
            int numRecordsPerSplit,
            int recordValueStartingOffset)
            throws Throwable {
        int counter = recordValueStartingOffset;
        String kafkaClusterId = getPubSubClusterId(kafkaClusterIdx);
        List<PscProducerMessage<String, Integer>> recordsForCluster = new ArrayList<>();
        for (int part = 0; part < numPartitions; part++) {
            for (int i = 0; i < numRecordsPerSplit; i++) {
                PscProducerMessage<String, Integer> message = new PscProducerMessage<>(
                        topic,
                        part,
                        topic + "-" + part,
                        counter++);
                message.setHeader("flink.kafka-cluster-name", kafkaClusterId.getBytes(StandardCharsets.UTF_8));
                recordsForCluster.add(message);
            }
        }

        produceToKafka(kafkaClusterIdx, recordsForCluster);

        return counter;
    }

    public static void produceToKafka(
            int kafkaClusterIdx, Collection<PscProducerMessage<String, Integer>> records)
            throws Throwable {
        produceToKafka(kafkaClusterIdx, records, StringSerializer.class, IntegerSerializer.class);
    }

    public static <K, V> void produceToKafka(
            int id,
            Collection<PscProducerMessage<K, V>> records,
            Class<? extends Serializer<K>> keySerializerClass,
            Class<? extends Serializer<V>> valueSerializerClass)
            throws Throwable {
        produceToKafka(
                kafkaClusters.get(id).getStandardProperties(),
                records,
                keySerializerClass,
                valueSerializerClass);
    }

    public static <K, V> void produceToKafka(
            Properties clusterProperties,
            Collection<PscProducerMessage<K, V>> records,
            Class<? extends Serializer<K>> keySerializerClass,
            Class<? extends Serializer<V>>
                    valueSerializerClass)
            throws Throwable {
        Properties props = new Properties();
        props.putAll(clusterProperties);
        props.setProperty(PscConfiguration.PSC_PRODUCER_ACKS, "all");
        props.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, keySerializerClass.getName());
        props.setProperty(
                PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, valueSerializerClass.getName());

        AtomicReference<Throwable> sendingError = new AtomicReference<>();
        Callback callback =
                (metadata, exception) -> {
                    if (exception != null) {
                        if (!sendingError.compareAndSet(null, exception)) {
                            sendingError.get().addSuppressed(exception);
                        }
                    }
                };
        try (PscProducer<K, V> producer = new PscProducer<>(PscConfigurationUtils.propertiesToPscConfiguration(props))) {
            for (PscProducerMessage<K, V> record : records) {
                producer.send(record, callback);
            }
            producer.flush();
        }
        if (sendingError.get() != null) {
            throw sendingError.get();
        }
    }
}
