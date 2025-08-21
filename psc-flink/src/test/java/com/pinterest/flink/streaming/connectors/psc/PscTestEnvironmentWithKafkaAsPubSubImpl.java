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

import com.pinterest.flink.connector.psc.source.PscSource;
import com.pinterest.flink.connector.psc.source.PscSourceBuilder;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.testutils.DockerImageVersions;
import com.pinterest.flink.connector.psc.testutils.PscUtil;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.flink.streaming.util.serialization.psc.KeyedSerializationSchema;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metrics.NullMetricsReporter;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import com.pinterest.psc.serde.ByteArraySerializer;
import kafka.server.KafkaServer;
import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * An implementation of the KafkaServerProvider.
 */
public class PscTestEnvironmentWithKafkaAsPubSubImpl extends PscTestEnvironmentWithKafkaAsPubSub {

    protected static final Logger LOG = LoggerFactory.getLogger(PscTestEnvironmentWithKafkaAsPubSubImpl.class);

    private static final String ZOOKEEPER_HOSTNAME = "zookeeper";
    private static final int ZOOKEEPER_PORT = 2181;
    private final Map<Integer, KafkaContainer> brokers = new HashMap<>();
    private final Set<Integer> pausedBroker = new HashSet<>();
    private @Nullable GenericContainer<?> zookeeper;
    private @Nullable Network network;
    private String brokerConnectionString = "";
    private Properties standardKafkaProperties;
    private Properties secureKafkaProperties;
    private Properties standardPscConsumerConfiguration;
    private Properties standardPscProducerConfiguration;
    private Properties pscDiscoveryConfiguration;
    private FlinkPscProducer.Semantic producerSemantic = FlinkPscProducer.Semantic.EXACTLY_ONCE;
    // 6 seconds is default. Seems to be too small for travis. 30 seconds
    private int zkTimeout = 30000;
    private Config config;
    private static final int REQUEST_TIMEOUT_SECONDS = 30;
    private static final int DELETE_TIMEOUT_SECONDS = 30;

    public void setProducerSemantic(FlinkPscProducer.Semantic producerSemantic) {
        this.producerSemantic = producerSemantic;
    }

    @Override
    public void prepare(Config config) throws Exception {
        // increase the timeout since in Travis ZK connection takes long time for secure
        // connection.
        if (config.isSecureMode()) {
            // run only one kafka server to avoid multiple ZK connections from many
            // instances - Travis timeout
            config.setKafkaServersNumber(1);
            zkTimeout = zkTimeout * 15;
        }
        this.config = config;
        brokers.clear();

        LOG.info("Starting KafkaServer");
        startKafkaContainerCluster(config.getKafkaServersNumber());
        LOG.info("KafkaServer started.");

        LOG.info("ZK and KafkaServer started.");

        standardKafkaProperties = new Properties();
        standardKafkaProperties.setProperty("bootstrap.servers", brokerConnectionString);
        standardKafkaProperties.setProperty("zookeeper.session.timeout.ms", String.valueOf(zkTimeout));
        standardKafkaProperties.setProperty("zookeeper.connection.timeout.ms", String.valueOf(zkTimeout));

        secureKafkaProperties = new Properties();
        secureKafkaProperties.setProperty("security.inter.broker.protocol", "SASL_PLAINTEXT");
        secureKafkaProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
        secureKafkaProperties.setProperty("sasl.kerberos.service.name", "kafka");
        secureKafkaProperties.setProperty("metadata.fetch.timeout.ms", "120000");

        standardPscConsumerConfiguration = new Properties();
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "flink-consumer-client");
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "flink-consumer-group");
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_METRIC_REPORTING_ENABLED, "false");
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, NullMetricsReporter.class.getName());
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST); // read from the beginning.
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_PARTITION_FETCH_MAX_BYTES, "256"); // make a lot of fetches (MESSAGES MUST BE SMALLER!)
        standardPscConsumerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "false");

        standardPscProducerConfiguration = new Properties();
        standardPscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "flink-producer-client");
        standardPscProducerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        standardPscProducerConfiguration.setProperty(PscConfiguration.PSC_METRIC_REPORTING_ENABLED, "false");
        standardPscProducerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, NullMetricsReporter.class.getName());
        standardPscProducerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "false");

        pscDiscoveryConfiguration = new Properties();
        int bootstrapCount = brokerConnectionString.split(",").length;
        pscDiscoveryConfiguration.setProperty("psc.discovery.topic.uri.prefixes",
                StringUtils.repeat(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, ",", bootstrapCount));
        pscDiscoveryConfiguration.setProperty("psc.discovery.connection.urls", brokerConnectionString);
        pscDiscoveryConfiguration.setProperty("psc.discovery.security.protocols",
                StringUtils.repeat("plaintext", ",", bootstrapCount));
    }

    @Override
    public void deleteTestTopic(String topic) {
        LOG.info("Deleting topic {}", topic);
        Properties properties = new Properties();
        properties.putAll(getStandardKafkaConfiguration());
        properties.putAll(getSecureKafkaConfiguration());
        String clientId = Long.toString(new Random().nextLong());
        properties.put("client.id", clientId);
        AdminClient adminClient = AdminClient.create(properties);
        // We do not use a try-catch clause here so we can apply a timeout to the admin
        // client closure.
        try {
            tryDelete(adminClient, topic);
        } catch (Exception e) {
            e.printStackTrace();
            fail(String.format("Delete test topic : %s failed, %s", topic, e.getMessage()));
        } finally {
            adminClient.close(Duration.ofMillis(5000L));
            maybePrintDanglingThreadStacktrace(clientId);
        }
    }

    private void tryDelete(AdminClient adminClient, String topic) throws Exception {
        try {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get(DELETE_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.info("Did not receive delete topic response within {} seconds. Checking if it succeeded",
                    DELETE_TIMEOUT_SECONDS);
            if (adminClient.listTopics().names().get(DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .contains(topic)) {
                throw new Exception("Topic still exists after timeout");
            }
        }
    }

    @Override
    public void createTestTopic(String topicUriString,
                                int numberOfPartitions,
                                int replicationFactor,
                                Properties properties) {
        createNewTopic(topicUriString, numberOfPartitions, replicationFactor, getStandardKafkaProperties());
    }

    public static void createNewTopic(
            String topicUriString, int numberOfPartitions, int replicationFactor, Properties properties) {
        LOG.info("Creating topic {}", topicUriString);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            Map<String, String> topicConfigs = new HashMap<>();
            topicConfigs.put("retention.ms", Long.toString(Long.MAX_VALUE));
            NewTopic topicObj = new NewTopic(BaseTopicUri.validate(topicUriString).getTopic(), numberOfPartitions, (short) replicationFactor).configs(topicConfigs);
            adminClient.createTopics(Collections.singleton(topicObj)).all().get();
        } catch (Exception e) {
            // try to create it assuming that it's not a topicUriString
            if (e instanceof TopicUriSyntaxException) {
                LOG.warn("Trying to create assuming that {} is just the topicName", topicUriString);
                try (AdminClient adminClient = AdminClient.create(properties)) {
                    NewTopic topicObj = new NewTopic(topicUriString, numberOfPartitions, (short) replicationFactor);
                    adminClient.createTopics(Collections.singleton(topicObj)).all().get();
                } catch (Exception e2) {
                    fail("Create test topic : " + topicUriString + " failed, " + e.getMessage());
                }
            } else {
                fail("Create test topic : " + topicUriString + " failed, " + e.getMessage());
            }
        }
    }

    public Properties getStandardKafkaConfiguration() {
        return standardKafkaProperties;
    }

    public Properties getStandardKafkaProperties() {
        return standardKafkaProperties;
    }

    public Properties getSecureKafkaConfiguration() {
        return config.isSecureMode() ? secureKafkaProperties : new Properties();
    }

    @Override
    public Properties getStandardPscConsumerConfiguration() {
        return standardPscConsumerConfiguration;
    }

    @Override
    public Properties getStandardPscProducerConfiguration() {
        return standardPscProducerConfiguration;
    }

    @Override
    public Properties getPscDiscoveryConfiguration() {
        return pscDiscoveryConfiguration;
    }

    private Properties getSecurePscClientConfiguration(String clientType) {
        Properties configuration = new Properties();
        if (config.isSecureMode()) {
            String prefix = String.format("psc.%s.", clientType);
            configuration.setProperty(prefix + "security.protocol", "SASL_PLAINTEXT");
            configuration.setProperty(prefix + "sasl.kerberos.service.name", "kafka");
        }
        return configuration;
    }

    public Properties getSecurePscProducerConfiguration() {
        return getSecurePscClientConfiguration("producer");
    }

    public Properties getSecurePscConsumerConfiguration() {
        return getSecurePscClientConfiguration("consumer");
    }

    @Override
    public String getBrokerConnectionString() {
        return brokerConnectionString;
    }

    @Override
    public String getVersion() {
        return "2.0";
    }

    @Override
    public <T> FlinkPscConsumerBase<T> getPscConsumer(List<String> topicUris,
                                                      PscDeserializationSchema<T> readSchema,
                                                      Properties configuration) {
//    List<String> collect = topicUris.stream()
//        .map(topicUri -> PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + topicUri)
//        .collect(Collectors.toList());
        return new FlinkPscConsumer<T>(topicUris, readSchema, configuration);
    }

    @Override
    public <T> PscSourceBuilder<T> getSourceBuilder(
            List<String> topics, PscDeserializationSchema<T> schema, Properties props) {
        return PscSource.<T>builder()
                .setTopicUris(topics)
                .setDeserializer(PscRecordDeserializationSchema.of(schema))
                .setProperties(props);
    }

    @Override
    public <K, V> Collection<PscConsumerMessage<K, V>> getAllMessagesFromTopicUri(
            Properties properties,
            String topicUri,
            int partition,
            long timeout
    ) throws ConsumerException, ConfigurationException {
        List<PscConsumerMessage<K, V>> result = new ArrayList<>();
        PscConfiguration pscConfiguration = new PscConfiguration();
        properties.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value));
        try (PscConsumer<K, V> consumer = new PscConsumer<>(pscConfiguration)) {
            consumer.assign(Arrays.asList(new TopicUriPartition(topicUri, partition)));

            while (true) {
                boolean processedAtLeastOneRecord = false;

                // wait for new records with timeout and break the loop if we didn't get any
                PscConsumerPollMessageIterator<K, V> iterator = consumer.poll(Duration.ofMillis(timeout));
                while (iterator.hasNext()) {
                    PscConsumerMessage<K, V> message = iterator.next();
                    result.add(message);
                    processedAtLeastOneRecord = true;
                }

                if (!processedAtLeastOneRecord) {
                    break;
                }
            }
            consumer.commitSync();
        }

        return UnmodifiableList.decorate(result);
    }

    @Override
    public <T> StreamSink<T> getProducerSink(String topicUri,
                                             SerializationSchema<T> serSchema,
                                             Properties configuration,
                                             FlinkPscPartitioner<T> partitioner) {
        return new StreamSink<>(new FlinkPscProducer<>(topicUri, serSchema, configuration, partitioner,
                producerSemantic, FlinkPscProducer.DEFAULT_PSC_PRODUCERS_POOL_SIZE));
    }

    @Override
    public <T> DataStreamSink<T> produceIntoKafka(DataStream<T> stream,
                                                  String topicUri,
                                                  KeyedSerializationSchema<T> serSchema,
                                                  Properties configuration,
                                                  FlinkPscPartitioner<T> partitioner) {
        return stream.addSink(
                new FlinkPscProducer<T>(topicUri, serSchema, configuration, Optional.ofNullable(partitioner),
                        producerSemantic, FlinkPscProducer.DEFAULT_PSC_PRODUCERS_POOL_SIZE));
    }

    @Override
    public <T> DataStreamSink<T> produceIntoKafka(DataStream<T> stream,
                                                  String topicUri,
                                                  SerializationSchema<T> serSchema,
                                                  Properties configuration,
                                                  FlinkPscPartitioner<T> partitioner) {
        return stream.addSink(new FlinkPscProducer<T>(topicUri, serSchema, configuration, partitioner,
                producerSemantic, FlinkPscProducer.DEFAULT_PSC_PRODUCERS_POOL_SIZE));
    }

    @Override
    public <T> DataStreamSink<T> produceIntoKafka(DataStream<T> stream,
                                                  String topicUri,
                                                  PscSerializationSchema<T> serSchema,
                                                  Properties configuration) {
        return stream.addSink(new FlinkPscProducer<T>(topicUri, serSchema, configuration, producerSemantic));
    }

    @Override
    public PscOffsetHandler createOffsetHandler(String topicUri) throws ConfigurationException, ConsumerException {
        return new PscOffsetHandlerImpl(topicUri);
    }

    @Override
    public void restartBroker(int leaderId) throws Exception {
        unpause(leaderId);
    }

    @Override
    public void stopBroker(int brokerId) throws Exception {
        pause(brokerId);
    }

    @Override
    public int getLeaderToShutDown(String topic) throws Exception {
        AdminClient client = AdminClient.create(getStandardKafkaProperties());
        TopicDescription result = client.describeTopics(Collections.singleton(topic)).all().get().get(topic);
        return result.partitions().get(0).leader().id();
    }

    @Override
    public int getBrokerId(KafkaServer server) {
        return server.config().brokerId();
    }

    @Override
    public boolean isSecureRunSupported() {
        return true;
    }

    @Override
    public void shutdown() throws Exception {
        brokers.values().forEach(GenericContainer::stop);
        brokers.clear();

        if (zookeeper != null) {
            zookeeper.stop();
        }

        if (network != null) {
            network.close();
        }

        super.shutdown();
    }

    @Override
    public void produceToKafka(String topicUri,
                               int numMessagesPerPartition,
                               int numPartitions,
                               String basePayload) throws ProducerException, ConfigurationException, IOException {
        PscConfiguration pscConfiguration = new PscConfiguration();
        standardPscProducerConfiguration.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value));
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getCanonicalName());
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class.getCanonicalName());
        PscProducer<byte[], byte[]> pscProducer = new PscProducer<>(pscConfiguration);
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessagesPerPartition; j++) {
                pscProducer.send(new PscProducerMessage<>(topicUri, (basePayload + i).getBytes()));
            }
        }
        pscProducer.close();
    }

    private class PscOffsetHandlerImpl implements PscOffsetHandler {

        private final PscConsumer<byte[], byte[]> offsetClient;

        public PscOffsetHandlerImpl(String topicUri) throws ConsumerException, ConfigurationException {
            PscConfiguration pscConfiguration = new PscConfiguration();
            standardPscConsumerConfiguration.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value));
            pscDiscoveryConfiguration.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value));
            pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, ByteArrayDeserializer.class.getName());
            pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, ByteArrayDeserializer.class.getName());
            offsetClient = new PscConsumer<>(pscConfiguration);
        }

        @Override
        public Long getCommittedOffset(String topicUri, int partition) throws ConfigurationException, ConsumerException {
            MessageId committed = offsetClient
                    .committed(new TopicUriPartition(topicUri, partition));
            return (committed != null) ? committed.getOffset() : null;
        }

        @Override
        public void setCommittedOffset(String topicUri, int partition, long offset) throws ConfigurationException, ConsumerException {
            // PSC assumes that the message id passed for offset commit is the last message id consumed, therefore to match
            // Kafka's committed offset sequencing, we would need to drop the offset by one (PSC will bump it again).
            MessageId messageId = new MessageId(new TopicUriPartition(topicUri, partition), offset - 1);
            offsetClient.commitSync(messageId);
        }

        @Override
        public void close() throws ConsumerException {
            offsetClient.close();
        }
    }

    private void startKafkaContainerCluster(int numBrokers) {
        if (numBrokers > 1) {
            network = Network.newNetwork();
            zookeeper = createZookeeperContainer(network);
            zookeeper.start();
            LOG.info("Zookeeper container started");
        }
        for (int brokerID = 0; brokerID < numBrokers; brokerID++) {
            KafkaContainer broker = createKafkaContainer(brokerID, zookeeper);
            brokers.put(brokerID, broker);
        }
        new ArrayList<>(brokers.values()).parallelStream().forEach(GenericContainer::start);
        LOG.info("{} brokers started: {}", numBrokers, brokers.keySet());
        brokerConnectionString =
                brokers.values().stream()
                        .map(KafkaContainer::getBootstrapServers)
                        // Here we have URL like "PLAINTEXT://127.0.0.1:15213", and we only keep the
                        // "127.0.0.1:15213" part in broker connection string
                        .map(server -> server.split("://")[1])
                        .collect(Collectors.joining(","));
    }

    private GenericContainer<?> createZookeeperContainer(Network network) {
        return new GenericContainer<>(DockerImageName.parse(DockerImageVersions.ZOOKEEPER))
                .withNetwork(network)
                .withNetworkAliases(ZOOKEEPER_HOSTNAME)
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_PORT));
    }

    private KafkaContainer createKafkaContainer(
            int brokerID, @Nullable GenericContainer<?> zookeeper) {
        String brokerName = String.format("Kafka-%d", brokerID);
        KafkaContainer broker =
                PscUtil.createKafkaContainer(DockerImageVersions.KAFKA, LOG, brokerName)
                        .withNetworkAliases(brokerName)
                        .withEnv("KAFKA_BROKER_ID", String.valueOf(brokerID))
                        .withEnv("KAFKA_MESSAGE_MAX_BYTES", String.valueOf(50 * 1024 * 1024))
                        .withEnv("KAFKA_REPLICA_FETCH_MAX_BYTES", String.valueOf(50 * 1024 * 1024))
                        .withEnv(
                                "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                                Integer.toString(1000 * 60 * 60 * 2))
                        // Disable log deletion to prevent records from being deleted during test
                        // run
                        .withEnv("KAFKA_LOG_RETENTION_MS", "-1")
                        .withEnv("KAFKA_ZOOKEEPER_SESSION_TIMEOUT_MS", String.valueOf(zkTimeout))
                        .withEnv(
                                "KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS", String.valueOf(zkTimeout));

        if (zookeeper != null) {
            broker.dependsOn(zookeeper)
                    .withNetwork(zookeeper.getNetwork())
                    .withExternalZookeeper(
                            String.format("%s:%d", ZOOKEEPER_HOSTNAME, ZOOKEEPER_PORT));
        } else {
            broker.withEmbeddedZookeeper();
        }
        return broker;
    }

    private void pause(int brokerId) {
        if (pausedBroker.contains(brokerId)) {
            LOG.warn("Broker {} is already paused. Skipping pause operation", brokerId);
            return;
        }
        DockerClientFactory.instance()
                .client()
                .pauseContainerCmd(brokers.get(brokerId).getContainerId())
                .exec();
        pausedBroker.add(brokerId);
        LOG.info("Broker {} is paused", brokerId);
    }

    private void unpause(int brokerId) throws Exception {
        if (!pausedBroker.contains(brokerId)) {
            LOG.warn("Broker {} is already running. Skipping unpause operation", brokerId);
            return;
        }
        DockerClientFactory.instance()
                .client()
                .unpauseContainerCmd(brokers.get(brokerId).getContainerId())
                .exec();
        try (AdminClient adminClient = AdminClient.create(getStandardKafkaProperties())) {
            CommonTestUtils.waitUtil(
                    () -> {
                        try {
                            return adminClient.describeCluster().nodes().get().stream()
                                    .anyMatch((node) -> node.id() == brokerId);
                        } catch (Exception e) {
                            return false;
                        }
                    },
                    Duration.ofSeconds(30),
                    String.format(
                            "The paused broker %d is not recovered within timeout", brokerId));
        }
        pausedBroker.remove(brokerId);
        LOG.info("Broker {} is resumed", brokerId);
    }
}
