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

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.IntegerDeserializer;
import com.pinterest.psc.serde.Serializer;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.pinterest.flink.connector.psc.testutils.PscTestUtils.putDiscoveryProperties;
import static org.junit.Assert.fail;

/**
 * The base for the PSC tests using Kafka as backend pubsub. It brings up:
 * <ul>
 *     <li>A ZooKeeper mini cluster</li>
 *     <li>Three Kafka Brokers (mini clusters)</li>
 *     <li>A Flink mini cluster</li>
 * </ul>
 *
 * <p>Code in this test is based on the following GitHub repository:
 * <a href="https://github.com/sakserv/hadoop-mini-clusters">
 *   https://github.com/sakserv/hadoop-mini-clusters</a> (ASL licensed),
 * as per commit <i>bc6b2b2d5f6424d5f377aa6c0871e82a956462ef</i></p>
 */
@SuppressWarnings("serial")
public abstract class PscTestBaseWithKafkaAsPubSub extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(PscTestBaseWithKafkaAsPubSub.class);

    protected static final int NUMBER_OF_KAFKA_SERVERS = 3;

    protected static String brokerConnectionStrings;

    protected static Properties standardKafkaProperties;

    protected static Properties secureKafkaProperties;

    protected static Properties standardPscConsumerConfiguration;

    protected static Properties securePscConsumerConfiguration;

    protected static Properties standardPscProducerConfiguration;

    protected static Properties securePscProducerConfiguration;

    protected static Properties pscDiscoveryConfiguration;

    protected static FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);

    protected static PscTestEnvironmentWithKafkaAsPubSub pscTestEnvWithKafka;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    // ------------------------------------------------------------------------
    //  Setup and teardown of the mini clusters
    // ------------------------------------------------------------------------

    @BeforeClass
    public static void prepare() throws Exception {
        prepare(true);
    }

    public static void prepare(boolean hideKafkaBehindProxy) throws Exception {
        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Starting PscTestBaseWithKafkaAsPubSub ");
        LOG.info("-------------------------------------------------------------------------");

        startClusters(false, hideKafkaBehindProxy);
    }

    @AfterClass
    public static void shutDownServices() throws Exception {

        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Shut down PscTestBaseWithKafkaAsPubSub ");
        LOG.info("-------------------------------------------------------------------------");

        TestStreamEnvironment.unsetAsContext();

        shutdownClusters();

        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    PscTestBaseWithKafkaAsPubSub finished");
        LOG.info("-------------------------------------------------------------------------");
    }

    protected static org.apache.flink.configuration.Configuration getFlinkConfiguration() {
        org.apache.flink.configuration.Configuration flinkConfig = new org.apache.flink.configuration.Configuration();
        flinkConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("16m"));
        flinkConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "my_reporter." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());
        return flinkConfig;
    }

    protected static void startClusters() throws Exception {
        startClusters(PscTestEnvironmentWithKafkaAsPubSub.createConfig().setKafkaServersNumber(NUMBER_OF_KAFKA_SERVERS));
    }

    protected static void startClusters(boolean secureMode, boolean hideKafkaBehindProxy) throws Exception {
        startClusters(PscTestEnvironmentWithKafkaAsPubSub.createConfig()
                .setKafkaServersNumber(NUMBER_OF_KAFKA_SERVERS)
                .setSecureMode(secureMode)
                .setHideKafkaBehindProxy(hideKafkaBehindProxy));
    }

    protected static void startClusters(PscTestEnvironmentWithKafkaAsPubSub.Config environmentConfig) throws Exception {
        pscTestEnvWithKafka = constructKafkaTestEnvionment();

        LOG.info("Starting PscTestBaseWithKafkaAsPubSub.prepare() for PSC " + pscTestEnvWithKafka.getVersion());

        pscTestEnvWithKafka.prepare(environmentConfig);

        standardKafkaProperties = pscTestEnvWithKafka.getStandardKafkaProperties();
        brokerConnectionStrings = pscTestEnvWithKafka.getBrokerConnectionString();
        secureKafkaProperties = pscTestEnvWithKafka.getSecureKafkaConfiguration();
        standardPscConsumerConfiguration = pscTestEnvWithKafka.getStandardPscConsumerConfiguration();
        securePscConsumerConfiguration = pscTestEnvWithKafka.getSecurePscConsumerConfiguration();
        standardPscProducerConfiguration = pscTestEnvWithKafka.getStandardPscProducerConfiguration();
        securePscProducerConfiguration = pscTestEnvWithKafka.getSecurePscProducerConfiguration();
        pscDiscoveryConfiguration = pscTestEnvWithKafka.getPscDiscoveryConfiguration();

        if (environmentConfig.isSecureMode()) {
            if (!pscTestEnvWithKafka.isSecureRunSupported()) {
                throw new IllegalStateException(
                        "Attempting to test in secure mode but secure mode not supported by the PscTestEnvironmentWithKafkaAsPubSub.");
            }
            secureKafkaProperties = pscTestEnvWithKafka.getSecureKafkaConfiguration();
        }
    }

    protected static PscTestEnvironmentWithKafkaAsPubSub constructKafkaTestEnvionment() throws Exception {
        Class<?> clazz = Class.forName(
                "com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSubImpl");
        return (PscTestEnvironmentWithKafkaAsPubSub) InstantiationUtil.instantiate(clazz);
    }

    protected static void shutdownClusters() throws Exception {
        if (secureKafkaProperties != null) {
            secureKafkaProperties.clear();
        }

        if (pscTestEnvWithKafka != null) {
            pscTestEnvWithKafka.shutdown();
        }
    }

    // ------------------------------------------------------------------------
    //  Execution utilities
    // ------------------------------------------------------------------------

    protected static void tryExecutePropagateExceptions(StreamExecutionEnvironment see, String name) throws Exception {
        try {
            see.execute(name);
        } catch (ProgramInvocationException | JobExecutionException root) {
            Throwable cause = root.getCause();

            // search for nested SuccessExceptions
            int depth = 0;
            while (!(cause instanceof SuccessException)) {
                if (cause == null || depth++ == 20) {
                    throw root;
                } else {
                    cause = cause.getCause();
                }
            }
        }
    }

    public static void createTestTopic(String topicUri, int numberOfPartitions, int replicationFactor) {
        pscTestEnvWithKafka.createTestTopic(topicUri, numberOfPartitions, replicationFactor);
    }

    protected static void deleteTestTopic(String topic) {
        pscTestEnvWithKafka.deleteTestTopic(topic);
    }

    public static <K, V> void produceMessages(Collection<PscProducerMessage<K, V>> messages,
                                              Class<? extends Serializer<K>> keySerializerClass,
                                              Class<? extends Serializer<V>> valueSerializerClass) throws Throwable {
        Properties props = new Properties();
        props.putAll(standardPscProducerConfiguration);
        props.putAll(pscTestEnvWithKafka.getIdempotentProducerConfig());
        props.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, keySerializerClass.getName());
        props.setProperty(
                PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, valueSerializerClass.getName());
        putDiscoveryProperties(props, brokerConnectionStrings, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX);

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
            for (PscProducerMessage<K, V> record : messages) {
                producer.send(record, callback);
            }
        }
        if (sendingError.get() != null) {
            throw sendingError.get();
        }
    }

    /**
     * We manually handle the timeout instead of using JUnit's timeout to return failure instead of timeout error.
     * After timeout we assume that there are missing messages and there is a bug, not that the test has run out of time.
     */
    protected void assertAtLeastOnceForTopic(
            Properties configuration,
            String topicUri,
            int partition,
            Set<Integer> expectedElements,
            long timeoutMillis) throws Exception {

        long startMillis = System.currentTimeMillis();
        Set<Integer> actualElements = new HashSet<>();

        // until we timeout...
        while (System.currentTimeMillis() < startMillis + timeoutMillis) {
            configuration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, IntegerDeserializer.class.getName());
            configuration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, IntegerDeserializer.class.getName());
            // We need to set these two properties so that they are lower than request.timeout.ms.
            configuration.setProperty("psc.consumer.session.timeout.ms", "2000");
            configuration.setProperty("psc.consumer.heartbeat.interval.ms", "500");

            // query for new messages ...
            Collection<PscConsumerMessage<Integer, Integer>> messages =
                    pscTestEnvWithKafka.getAllMessagesFromTopicUri(configuration, topicUri, partition, 100);

            for (PscConsumerMessage<Integer, Integer> message : messages) {
                actualElements.add(message.getValue());
            }

            // succeed if we got all expectedElements
            if (actualElements.containsAll(expectedElements)) {
                return;
            }
        }

        fail(String.format("Expected to contain all of: <%s>, but was: <%s>", expectedElements, actualElements));
    }

    protected void assertExactlyOnceForTopicUri(
            Properties configuration,
            String topicUri,
            int partition,
            List<Integer> expectedElements) throws ConsumerException, ConfigurationException {
        assertExactlyOnceForTopicUri(configuration, topicUri, partition, expectedElements, 30_000L);
    }

    /**
     * We manually handle the timeout instead of using JUnit's timeout to return failure instead of timeout error.
     * After timeout we assume that there are missing messages and there is a bug, not that the test has run out of time.
     */
    protected void assertExactlyOnceForTopicUri(
            Properties configuration,
            String topicUri,
            int partition,
            List<Integer> expectedElements,
            long timeoutMillis) throws ConfigurationException, ConsumerException {

        long startMillis = System.currentTimeMillis();
        List<Integer> actualElements = new ArrayList<>();

        Properties consumerConfiguration = new Properties();
        consumerConfiguration.putAll(configuration);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, IntegerDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, IntegerDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL, PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL_TRANSACTIONAL);

        // until we timeout...
        while (System.currentTimeMillis() < startMillis + timeoutMillis) {
            // query for new messages ...
            Collection<PscConsumerMessage<Integer, Integer>> messages = pscTestEnvWithKafka.getAllMessagesFromTopicUri(
                    consumerConfiguration, topicUri, partition, 1000);

            for (PscConsumerMessage<Integer, Integer> message : messages) {
                actualElements.add(message.getValue());
            }

            // succeed if we got all expectedElements
            if (actualElements.equals(expectedElements)) {
                return;
            }
            // fail early if we already have too many elements
            if (actualElements.size() > expectedElements.size()) {
                break;
            }
        }

        fail(String.format("Expected %s, but was: %s", formatElements(expectedElements), formatElements(actualElements)));
    }

    private String formatElements(List<Integer> elements) {
        if (elements.size() > 50) {
            return String.format("number of elements: <%s>", elements.size());
        } else {
            return String.format("elements: <%s>", elements);
        }
    }
}
