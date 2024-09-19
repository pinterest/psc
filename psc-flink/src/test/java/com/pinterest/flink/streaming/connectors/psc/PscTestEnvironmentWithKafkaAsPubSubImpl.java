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
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
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
import com.pinterest.psc.metrics.NullMetricsReporter;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import com.pinterest.psc.serde.ByteArraySerializer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import com.pinterest.flink.streaming.util.serialization.psc.KeyedSerializationSchema;
import org.apache.flink.util.NetUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.NetUtils.hostAndPortToUrlString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An implementation of the KafkaServerProvider.
 */
public class PscTestEnvironmentWithKafkaAsPubSubImpl extends PscTestEnvironmentWithKafkaAsPubSub {

    protected static final Logger LOG = LoggerFactory.getLogger(PscTestEnvironmentWithKafkaAsPubSubImpl.class);
    private final List<KafkaServer> brokers = new ArrayList<>();
    private File tmpZkDir;
    private File tmpKafkaParent;
    private List<File> tmpKafkaDirs;
    private TestingServer zookeeper;
    private String zookeeperConnectionString;
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

        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + (UUID.randomUUID().toString()));
        assertTrue("cannot create zookeeper temp dir", tmpZkDir.mkdirs());

        tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir-" + (UUID.randomUUID().toString()));
        assertTrue("cannot create kafka temp dir", tmpKafkaParent.mkdirs());

        tmpKafkaDirs = new ArrayList<>(config.getKafkaServersNumber());
        for (int i = 0; i < config.getKafkaServersNumber(); i++) {
            File tmpDir = new File(tmpKafkaParent, "server-" + i);
            assertTrue("cannot create kafka temp dir", tmpDir.mkdir());
            tmpKafkaDirs.add(tmpDir);
        }

        zookeeper = null;
        brokers.clear();

        zookeeper = new TestingServer(-1, tmpZkDir);
        zookeeperConnectionString = zookeeper.getConnectString();
        LOG.info("Starting Zookeeper with zookeeperConnectionString: {}", zookeeperConnectionString);

        LOG.info("Starting KafkaServer");

        ListenerName listenerName = ListenerName.forSecurityProtocol(
                config.isSecureMode() ? SecurityProtocol.SASL_PLAINTEXT : SecurityProtocol.PLAINTEXT);
        for (int i = 0; i < config.getKafkaServersNumber(); i++) {
            KafkaServer kafkaServer = getKafkaServer(i, tmpKafkaDirs.get(i));
            brokers.add(kafkaServer);
            brokerConnectionString += hostAndPortToUrlString(KAFKA_HOST,
                    kafkaServer.socketServer().boundPort(listenerName));
            brokerConnectionString += ",";
        }
        brokerConnectionString = String.join(",", brokerConnectionString.split(","));

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
                StringUtils.repeat(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX, ",", bootstrapCount));
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
        LOG.info("Creating topic {}", topicUriString);
        try (AdminClient adminClient = AdminClient.create(getStandardKafkaProperties())) {
            NewTopic topicObj = new NewTopic(BaseTopicUri.validate(topicUriString).getTopic(), numberOfPartitions, (short) replicationFactor);
            adminClient.createTopics(Collections.singleton(topicObj)).all().get();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Create test topic : " + topicUriString + " failed, " + e.getMessage());
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
    public List<KafkaServer> getBrokers() {
        return brokers;
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
        brokers.set(leaderId, getKafkaServer(leaderId, tmpKafkaDirs.get(leaderId)));
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
        for (KafkaServer broker : brokers) {
            if (broker != null) {
                broker.shutdown();
            }
        }
        brokers.clear();

        if (zookeeper != null) {
            try {
                zookeeper.stop();
            } catch (Exception e) {
                LOG.warn("ZK.stop() failed", e);
            }
            zookeeper = null;
        }

        // clean up the temp spaces

        if (tmpKafkaParent != null && tmpKafkaParent.exists()) {
            try {
                FileUtils.deleteDirectory(tmpKafkaParent);
            } catch (Exception e) {
                // ignore
            }
        }
        if (tmpZkDir != null && tmpZkDir.exists()) {
            try {
                FileUtils.deleteDirectory(tmpZkDir);
            } catch (Exception e) {
                // ignore
            }
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

    protected KafkaServer getKafkaServer(int brokerId, File tmpFolder) throws Exception {
        Properties kafkaProperties = new Properties();

        // properties have to be Strings
        kafkaProperties.put("advertised.host.name", KAFKA_HOST);
        kafkaProperties.put("broker.id", Integer.toString(brokerId));
        kafkaProperties.put("log.dir", tmpFolder.toString());
        kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
        kafkaProperties.put("message.max.bytes", String.valueOf(50 * 1024 * 1024));
        kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024));
        kafkaProperties.put("transaction.max.timeout.ms", Integer.toString(1000 * 60 * 60 * 2)); // 2hours

        // for CI stability, increase zookeeper session timeout
        kafkaProperties.put("zookeeper.session.timeout.ms", zkTimeout);
        kafkaProperties.put("zookeeper.connection.timeout.ms", zkTimeout);
        if (config.getKafkaServerProperties() != null) {
            kafkaProperties.putAll(config.getKafkaServerProperties());
        }

        final int numTries = 5;

        for (int i = 1; i <= numTries; i++) {
            int kafkaPort = NetUtils.getAvailablePort().getPort();
            kafkaProperties.put("port", Integer.toString(kafkaPort));

            // to support secure kafka cluster
            if (config.isSecureMode()) {
                LOG.info("Adding Kafka secure configurations");
                kafkaProperties.put("listeners", "SASL_PLAINTEXT://" + KAFKA_HOST + ":" + kafkaPort);
                kafkaProperties.put("advertised.listeners",
                        "SASL_PLAINTEXT://" + KAFKA_HOST + ":" + kafkaPort);
                kafkaProperties.putAll(getSecureKafkaConfiguration());
            }

            KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

            try {
                scala.Option<String> stringNone = scala.Option.apply(null);
                KafkaServer server = new KafkaServer(kafkaConfig, Time.SYSTEM, stringNone, false);
                server.startup();
                return server;
            } catch (KafkaException e) {
                if (e.getCause() instanceof BindException) {
                    // port conflict, retry...
                    LOG.info("Port conflict when starting Kafka Broker. Retrying...");
                } else {
                    throw e;
                }
            }
        }

        throw new Exception(
                "Could not start Kafka after " + numTries + " retries due to port conflicts.");
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
}
