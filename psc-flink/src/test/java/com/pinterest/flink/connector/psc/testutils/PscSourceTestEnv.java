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

package com.pinterest.flink.connector.psc.testutils;

import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.streaming.connectors.psc.PscTestBaseWithKafkaAsPubSub;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.IntegerDeserializer;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Base class for KafkaSource unit tests. */
public class PscSourceTestEnv extends PscTestBaseWithKafkaAsPubSub {
    public static final String GROUP_ID = "PscSourceTestEnv";
    public static final int NUM_PARTITIONS = 10;
    public static final int NUM_RECORDS_PER_PARTITION = 10;

    private static AdminClient adminClient;
    private static PscConsumer<String, Integer> consumer;

    public static void setup() throws Throwable {
        prepare();
        adminClient = getAdminClient();
        consumer = getConsumer();
    }

    public static void tearDown() throws Exception {
        consumer.close();
        adminClient.close();
        shutDownServices();
    }

    // --------------------- public client related helpers ------------------

    public static AdminClient getAdminClient() {
        Properties props = new Properties();
        props.putAll(standardKafkaProperties);
        return AdminClient.create(props);
    }

    public static PscConsumer<String, Integer> getConsumer() throws ConfigurationException, ConsumerException {
        Properties props = new Properties();
        props.putAll(standardPscConsumerConfiguration);
        props.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, GROUP_ID);
        props.setProperty(
                PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        props.setProperty(
                PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER,
                IntegerDeserializer.class.getName());
        return new PscConsumer<>(PscConfigurationUtils.propertiesToPscConfiguration(props));
    }

    public static Properties getConsumerProperties(Class<?> deserializerClass) {
        Properties props = new Properties();
        props.putAll(standardPscConsumerConfiguration);
        props.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, GROUP_ID);
        props.setProperty(
                PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, deserializerClass.getName());
        props.setProperty(
                PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, deserializerClass.getName());
        return props;
    }

    // ------------------- topic information helpers -------------------

    public static Map<Integer, Map<String, PscTopicUriPartitionSplit>> getSplitsByOwners(
            final Collection<String> topics, final int numSubtasks) throws ConfigurationException, ConsumerException {
        final Map<Integer, Map<String, PscTopicUriPartitionSplit>> splitsByOwners = new HashMap<>();
        for (String topic : topics) {
            getPartitionsForTopic(topic)
                    .forEach(
                            tp -> {
                                int ownerReader = Math.abs(tp.hashCode()) % numSubtasks;
                                PscTopicUriPartitionSplit split =
                                        new PscTopicUriPartitionSplit(
                                                tp,
                                                getEarliestOffset(tp),
                                                (long) NUM_RECORDS_PER_PARTITION);
                                splitsByOwners
                                        .computeIfAbsent(ownerReader, r -> new HashMap<>())
                                        .put(PscTopicUriPartitionSplit.toSplitId(tp), split);
                            });
        }
        return splitsByOwners;
    }

    /**
     * For a given partition {@code TOPIC-PARTITION} the {@code i}-th records looks like following.
     *
     * <pre>{@code
     * topic: TOPIC
     * partition: PARTITION
     * timestamp: 1000 * PARTITION
     * key: TOPIC-PARTITION
     * value: i
     * }</pre>
     */
    public static List<PscProducerMessage<String, Integer>> getRecordsForPartition(TopicUriPartition tp) {
        List<PscProducerMessage<String, Integer>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            records.add(
                    new PscProducerMessage<>(tp.getTopicUriAsString(), tp.getPartition(), tp.toString(), i, i * 1000L));
        }
        return records;
    }

    /**
     * For a given partition {@code TOPIC-PARTITION} the {@code i}-th records looks like following.
     *
     * <pre>{@code
     * topic: TOPIC
     * partition: PARTITION
     * timestamp: null
     * key: TOPIC-PARTITION
     * value: i
     * }</pre>
     */
    public static List<PscProducerMessage<String, Integer>> getRecordsForPartitionWithoutTimestamp(
            TopicUriPartition tp) {
        List<PscProducerMessage<String, Integer>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            records.add(new PscProducerMessage<>(tp.getTopicUriAsString(), tp.getPartition(), tp.toString(), i, null));
        }
        return records;
    }

    public static List<PscProducerMessage<String, Integer>> getRecordsForTopic(String topicUriString) {
        List<PscProducerMessage<String, Integer>> records = new ArrayList<>();
        for (TopicUriPartition tp : getPartitionsForTopic(topicUriString)) {
            records.addAll(getRecordsForPartition(tp));
        }
        return records;
    }

    public static List<PscProducerMessage<String, Integer>> getRecordsForTopicWithoutTimestamp(
            String topicUriString) {
        List<PscProducerMessage<String, Integer>> records = new ArrayList<>();
        for (TopicUriPartition tp : getPartitionsForTopic(topicUriString)) {
            records.addAll(getRecordsForPartitionWithoutTimestamp(tp));
        }
        return records;
    }

    public static List<TopicUriPartition> getPartitionsForTopics(Collection<String> topicUriStrings) {
        List<TopicUriPartition> partitions = new ArrayList<>();
        topicUriStrings.forEach(t -> {
            partitions.addAll(getPartitionsForTopic(t));
        });
        return partitions;
    }

    public static List<TopicUriPartition> getPartitionsForTopic(String topicUriString) {
        try {
            Set<TopicUriPartition> partitions = consumer.getPartitions(topicUriString);
            return new ArrayList<>(partitions);
        } catch (ConsumerException | ConfigurationException e) {
            throw new RuntimeException(e);
        }

    }

    public static Map<TopicUriPartition, Long> getEarliestOffsets(List<TopicUriPartition> partitions) {
        Map<TopicUriPartition, Long> earliestOffsets = new HashMap<>();
        for (TopicUriPartition tp : partitions) {
            earliestOffsets.put(tp, getEarliestOffset(tp));
        }
        return earliestOffsets;
    }

    public static Map<TopicUriPartition, Long> getCommittedOffsets(
            List<TopicUriPartition> partitions) {
        Map<TopicUriPartition, Long> committedOffsets = new HashMap<>();
        for (TopicUriPartition tp : partitions) {
            committedOffsets.put(tp, ((long) tp.getPartition() + 2));
        }
        return committedOffsets;
    }

    public static long getEarliestOffset(TopicUriPartition tp) {
        return tp.getPartition();
    }

    // --------------- topic manipulation helpers ---------------

    public static void createTestTopic(String topicUriString) {
        createTestTopic(topicUriString, NUM_PARTITIONS, 1);
    }

    public static void setupEarliestOffsets(String topicUriString) throws Throwable {
        // Delete some records to move the starting partition.
        List<TopicUriPartition> partitions = getPartitionsForTopic(topicUriString);
        setupEarliestOffsets(partitions);
    }

    public static void setupEarliestOffsets(List<TopicUriPartition> partitions) throws Throwable {
        Map<TopicPartition, RecordsToDelete> toDelete = new HashMap<>();
        getEarliestOffsets(partitions)
                .forEach((tp, offset) -> toDelete.put(new TopicPartition(tp.getTopicUri().getTopic(), tp.getPartition()), RecordsToDelete.beforeOffset(offset)));
        adminClient.deleteRecords(toDelete).all().get();
    }

    public static void setupCommittedOffsets(String topicUriString)
            throws ExecutionException, InterruptedException, ConfigurationException, ConsumerException {
        List<TopicUriPartition> partitions = getPartitionsForTopic(topicUriString);
        Map<TopicUriPartition, Long> committedOffsets = getCommittedOffsets(partitions);
        List<MessageId> toCommit = new ArrayList<>();
        for (Map.Entry<TopicUriPartition, Long> entry : committedOffsets.entrySet()) {
            toCommit.add(new MessageId(entry.getKey().getTopicUriAsString(), entry.getKey().getPartition(), entry.getValue()));
        }
        consumer.commitSync(toCommit);
        Map<TopicPartition, OffsetAndMetadata> toVerify =
                adminClient
                        .listConsumerGroupOffsets(
                                GROUP_ID,
                                new ListConsumerGroupOffsetsOptions()
                                        .topicPartitions(
                                                committedOffsets.keySet().stream().map(tup -> new TopicPartition(tup.getTopicUri().getTopic(), tup.getPartition())).collect(Collectors.toList())))
                        .partitionsToOffsetAndMetadata()
                        .get();
        assertEquals("The offsets are not committed", committedOffsets, toVerify);
    }

    public static void produceMessages(Collection<PscProducerMessage<String, Integer>> records)
            throws Throwable {
        produceMessages(records, StringSerializer.class, IntegerSerializer.class);
    }

    public static void setupTopic(
            String topicUriString,
            boolean setupEarliestOffsets,
            boolean setupCommittedOffsets,
            Function<String, List<PscProducerMessage<String, Integer>>> testDataGenerator)
            throws Throwable {
        createTestTopic(topicUriString);
        produceMessages(testDataGenerator.apply(topicUriString));
        if (setupEarliestOffsets) {
            setupEarliestOffsets(topicUriString);
        }
        if (setupCommittedOffsets) {
            setupCommittedOffsets(topicUriString);
        }
    }
}
