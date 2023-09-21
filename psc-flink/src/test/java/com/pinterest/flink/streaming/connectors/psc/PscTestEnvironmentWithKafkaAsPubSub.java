/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.streaming.connectors.psc.internals.PscDeserializationSchemaWrapper;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import kafka.server.KafkaServer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.networking.NetworkFailuresProxy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import com.pinterest.flink.streaming.util.serialization.psc.KeyedSerializationSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Abstract class providing a Kafka test environment.
 */
public abstract class PscTestEnvironmentWithKafkaAsPubSub {
    public static String PSC_TEST_TOPIC_URI_PREFIX = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region1::cluster1:";

    /**
     * Configuration class for {@link PscTestEnvironmentWithKafkaAsPubSub}.
     */
    public static class Config {
        private int kafkaServersNumber = 1;
        private Properties kafkaServerProperties = null;
        private boolean secureMode = false;
        private boolean hideKafkaBehindProxy = false;

        /**
         * Please use {@link PscTestEnvironmentWithKafkaAsPubSub#createConfig()} method.
         */
        private Config() {
        }

        public int getKafkaServersNumber() {
            return kafkaServersNumber;
        }

        public Config setKafkaServersNumber(int kafkaServersNumber) {
            this.kafkaServersNumber = kafkaServersNumber;
            return this;
        }

        public Properties getKafkaServerProperties() {
            return kafkaServerProperties;
        }

        public Config setKafkaServerProperties(Properties kafkaServerProperties) {
            this.kafkaServerProperties = kafkaServerProperties;
            return this;
        }

        public boolean isSecureMode() {
            return secureMode;
        }

        public Config setSecureMode(boolean secureMode) {
            this.secureMode = secureMode;
            return this;
        }

        public boolean isHideKafkaBehindProxy() {
            return hideKafkaBehindProxy;
        }

        public Config setHideKafkaBehindProxy(boolean hideKafkaBehindProxy) {
            this.hideKafkaBehindProxy = hideKafkaBehindProxy;
            return this;
        }
    }

    protected static final String KAFKA_HOST = "localhost";

    protected final List<NetworkFailuresProxy> networkFailuresProxies = new ArrayList<>();

    public static Config createConfig() {
        return new Config();
    }

    public abstract void prepare(Config config) throws Exception;

    public void shutdown() throws Exception {
        for (NetworkFailuresProxy proxy : networkFailuresProxies) {
            proxy.close();
        }
    }

    public abstract void deleteTestTopic(String topic);

    public abstract void createTestTopic(String topic, int numberOfPartitions, int replicationFactor, Properties properties);

    public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
        this.createTestTopic(topic, numberOfPartitions, replicationFactor, new Properties());
    }

    public abstract Properties getStandardKafkaProperties();

    public abstract Properties getSecureKafkaConfiguration();

    public abstract Properties getStandardPscConsumerConfiguration();

    public abstract Properties getSecurePscConsumerConfiguration();

    public abstract Properties getStandardPscProducerConfiguration();

    public abstract Properties getSecurePscProducerConfiguration();

    public abstract Properties getPscDiscoveryConfiguration();

    public abstract String getBrokerConnectionString();

    public abstract String getVersion();

    public abstract List<KafkaServer> getBrokers();

    public Properties getIdempotentProducerConfig() {
        Properties configuration = new Properties();
        configuration.setProperty(PscConfiguration.PSC_PRODUCER_IDEMPOTENCE_ENABLED, "true");
        configuration.setProperty(PscConfiguration.PSC_PRODUCER_ACKS, "all");
        configuration.setProperty(PscConfiguration.PSC_PRODUCER_RETRIES, "3");
        return configuration;
    }

    // -- consumer / producer instances:
    public <T> FlinkPscConsumerBase<T> getPscConsumer(List<String> topicUris, DeserializationSchema<T> deserializationSchema, Properties configuration) {
        return getPscConsumer(topicUris, new PscDeserializationSchemaWrapper<>(deserializationSchema), configuration);
    }

    public <T> FlinkPscConsumerBase<T> getPscConsumer(String topicUri, PscDeserializationSchema<T> readSchema, Properties configuration) {
        return getPscConsumer(Collections.singletonList(topicUri), readSchema, configuration);
    }

    public <T> FlinkPscConsumerBase<T> getPscConsumer(String topicUri, DeserializationSchema<T> deserializationSchema, Properties configuration) {
        return getPscConsumer(Collections.singletonList(topicUri), deserializationSchema, configuration);
    }

    public abstract <T> FlinkPscConsumerBase<T> getPscConsumer(List<String> topicUris, PscDeserializationSchema<T> readSchema, Properties configuration);

    public abstract <K, V> Collection<PscConsumerMessage<K, V>> getAllMessagesFromTopicUri(
            Properties configuration,
            String topicUri,
            int partition,
            long timeout) throws ConsumerException, ConfigurationException;

    public abstract <T> StreamSink<T> getProducerSink(
            String topicUri,
            SerializationSchema<T> serSchema,
            Properties configuration,
            FlinkPscPartitioner<T> partitioner);

    @Deprecated
    public abstract <T> DataStreamSink<T> produceIntoKafka(
            DataStream<T> stream,
            String topicUri,
            KeyedSerializationSchema<T> serSchema,
            Properties configuration,
            FlinkPscPartitioner<T> partitioner);

    public abstract <T> DataStreamSink<T> produceIntoKafka(
            DataStream<T> stream,
            String topicUri,
            SerializationSchema<T> serSchema,
            Properties configuration,
            FlinkPscPartitioner<T> partitioner);

    public <T> DataStreamSink<T> produceIntoKafka(DataStream<T> stream, String topicUri,
                                                  PscSerializationSchema<T> serSchema, Properties configuration) {
        throw new RuntimeException("PscSerializationSchema is only supported on the modern PSC Connector.");
    }

    // -- offset handlers

    /**
     * Simple interface to commit and retrieve offsets.
     */
    public interface PscOffsetHandler {
        Long getCommittedOffset(String topicUri, int partition) throws ConfigurationException, ConsumerException;

        void setCommittedOffset(String topicUri, int partition, long offset) throws ConfigurationException, ConsumerException;

        void close() throws ConsumerException;
    }

    public abstract PscOffsetHandler createOffsetHandler(String topicUri) throws ConfigurationException, ConsumerException;

    // -- leader failure simulation

    public abstract void restartBroker(int leaderId) throws Exception;

    public abstract int getLeaderToShutDown(String topicUri) throws Exception;

    public abstract int getBrokerId(KafkaServer server);

    public abstract boolean isSecureRunSupported();

    public void blockProxyTraffic() {
        for (NetworkFailuresProxy proxy : networkFailuresProxies) {
            proxy.blockTraffic();
        }
    }

    public void unblockProxyTraffic() {
        for (NetworkFailuresProxy proxy : networkFailuresProxies) {
            proxy.unblockTraffic();
        }
    }

    protected NetworkFailuresProxy createProxy(String remoteHost, int remotePort) {
        NetworkFailuresProxy proxy = new NetworkFailuresProxy(0, remoteHost, remotePort);
        networkFailuresProxies.add(proxy);
        return proxy;
    }

    protected void maybePrintDanglingThreadStacktrace(String threadNameKeyword) {
        for (Map.Entry<Thread, StackTraceElement[]> threadEntry : Thread.getAllStackTraces().entrySet()) {
            if (threadEntry.getKey().getName().contains(threadNameKeyword)) {
                System.out.println("Dangling thread found:");
                for (StackTraceElement ste : threadEntry.getValue()) {
                    System.out.println(ste);
                }
            }
        }
    }

    public abstract void produceToKafka(String topicUri,
                                        int numMessagesPerPartition,
                                        int numPartitions,
                                        String basePayload) throws ProducerException, ConfigurationException;
}
