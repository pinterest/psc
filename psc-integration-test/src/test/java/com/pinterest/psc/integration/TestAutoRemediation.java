package com.pinterest.psc.integration;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.consumer.PscConsumerUtils;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.integration.consumer.TestOneKafkaBackend;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.metrics.PscMetricsUtil;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.producer.PscProducerUtils;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestAutoRemediation {
    private static final PscLogger logger = PscLogger.getLogger(TestOneKafkaBackend.class);
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokers(1)
            .withBrokerProperty("auto.create.topics.enable", "false");


    private static AdminClient adminClient;
    private static final int TEST_TIMEOUT_SECONDS = 60;
    private static final PscConfiguration pscConsumerConfiguration = new PscConfiguration();
    private static final PscConfiguration pscProducerConfiguration = new PscConfiguration();
    private static PscConfigurationInternal producerInternalConfiguration;
    private static PscConfigurationInternal consumerInternalConfiguration;
    private static String baseConsumerClientId;
    private static String baseConsumerGroupId;
    private static String baseProducerClientId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 12;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 1;
    private KafkaCluster kafkaCluster;
    private String topicRnStr1, topicUriStr1, topicRnStr2, topicUriStr2;

    @BeforeAll
    public static void prepare() throws Exception {
        adminClient = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient();
    }

    @AfterAll
    public static void shutdown() throws IOException {
        adminClient.close();
    }

    /**
     * Initializes a Kafka cluster that is commonly used by all tests, and creates some topics on it.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeEach
    public void setup() throws IOException, InterruptedException, ConfigurationException {
        baseConsumerClientId = this.getClass().getSimpleName() + "-psc-consumer-client";
        baseConsumerGroupId = this.getClass().getSimpleName() + "-psc-consumer-group";
        pscConsumerConfiguration.clear();
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseConsumerClientId + "-" + UUID.randomUUID());
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, baseConsumerGroupId + "-" + UUID.randomUUID());
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        consumerInternalConfiguration = new PscConfigurationInternal(pscConsumerConfiguration, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);

        baseProducerClientId = this.getClass().getSimpleName() + "-psc-producer-client";
        pscProducerConfiguration.clear();
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, baseProducerClientId + "-" + UUID.randomUUID());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        producerInternalConfiguration = new PscConfigurationInternal(pscProducerConfiguration, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);

        kafkaCluster = new KafkaCluster(
                "plaintext",
                "region",
                "cluster",
                Integer.parseInt(
                        sharedKafkaTestResource.getKafkaBrokers().stream().iterator().next().getConnectString().split(":")[2]
                )
        );
        topicRnStr1 = String.format("%s:kafka:env:cloud_%s::%s:%s",
                TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), topic1);
        topicUriStr1 = String.format("%s:%s%s", kafkaCluster.getTransport(), TopicUri.SEPARATOR, topicRnStr1);

        topicRnStr2 = String.format("%s:kafka:env:cloud_%s::%s:%s",
                TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), topic2);
        topicUriStr2 = String.format("%s:%s%s", kafkaCluster.getTransport(), TopicUri.SEPARATOR, topicRnStr2);

        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic1, partitions1);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic2, partitions2);
        PscMetricsUtil.cleanup(PscMetricRegistryManager.getInstance());
    }

    /**
     * Deleted the topic that is created by default. Also, adds a slight delay to make sure cleanup is complete
     * when tests run consecutively.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource, topic1);
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource, topic2);
        Thread.sleep(1000);
    }

    /**
     * Verifies that backend consumer does not attempt any retries in case of errors if auto resolution is disabled. In
     * this case, the consumer starts consuming without an offset reset policy, which leads to a
     * {@code NoOffsetForPartitionException} exception.
     *
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testDisabledRetriesForConsumer() throws ConfigurationException, ConsumerException {
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "false");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "new_topic_group");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE);

        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConsumerConfiguration);

        assertFalse(PscConsumerUtils.getPscConfigurationInternal(pscConsumer).isAutoResolutionEnabled());
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));

        try {
            while (pscConsumer.assignment().isEmpty())
                pscConsumer.poll();
            fail("Expected poll to fail since there is no viable offset reset policy configured. position: " +
                    pscConsumer.position(new TopicUriPartition(topicUriStr1, 0)));
        } catch (Exception e) {
            assertEquals(ConsumerException.class, e.getClass());
            assertEquals(NoOffsetForPartitionException.class, e.getCause().getClass());
            // OK - verify that retries were performed
            assertEquals(
                    0,
                    PscMetricRegistryManager.getInstance().getBackendCounterMetric(null, PscMetrics.PSC_CONSUMER_RETRIES_METRIC, consumerInternalConfiguration)
            );
            assertEquals(
                    0,
                    PscMetricRegistryManager.getInstance().getBackendCounterMetric(null, PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_FAILURE + "." + NoOffsetForPartitionException.class.getName(), consumerInternalConfiguration)
            );
            assertEquals(
                    0,
                    PscMetricRegistryManager.getInstance().getBackendCounterMetric(null, PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + NoOffsetForPartitionException.class.getName(), consumerInternalConfiguration)
            );
        }

        pscConsumer.close();
    }

    /**
     * Verifies that backend consumer retries are done adequate number of times when particular exceptions (for
     * which retries are justified) occur. In this case, the consumer starts consuming without an offset reset policy,
     * which leads to a {@code NoOffsetForPartitionException} exception.
     *
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testRetriesOnNoOffsetForPartitionException() throws ConfigurationException, ConsumerException {
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, "5");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "new_topic_group");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE);

        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConsumerConfiguration);

        assertTrue(PscConsumerUtils.getPscConfigurationInternal(pscConsumer).isAutoResolutionEnabled());
        int autoResolutionRetryCount = PscConsumerUtils.getPscConfigurationInternal(pscConsumer).getAutoResolutionRetryCount();

        pscConsumer.subscribe(Collections.singleton(topicUriStr1));

        try {
            while (pscConsumer.assignment().isEmpty())
                pscConsumer.poll();
            fail("Expected poll to fail since there is no viable offset reset policy configured. position: " +
                    pscConsumer.position(new TopicUriPartition(topicUriStr1, 0)));
        } catch (Exception e) {
            assertEquals(ConsumerException.class, e.getClass());
            assertEquals(NoOffsetForPartitionException.class, e.getCause().getClass());
            // OK - verify that retries were performed
            assertEquals(
                    autoResolutionRetryCount,
                    PscMetricRegistryManager.getInstance().getBackendCounterMetric(null, PscMetrics.PSC_CONSUMER_RETRIES_METRIC, consumerInternalConfiguration)
            );
            assertEquals(
                    autoResolutionRetryCount - 1,
                    PscMetricRegistryManager.getInstance().getBackendCounterMetric(null, PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_FAILURE + "." + NoOffsetForPartitionException.class.getName(), consumerInternalConfiguration)
            );
            assertEquals(
                    0,
                    PscMetricRegistryManager.getInstance().getBackendCounterMetric(null, PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + NoOffsetForPartitionException.class.getName(), consumerInternalConfiguration)
            );
            assertEquals(
                    1,
                    PscMetricRegistryManager.getInstance().getBackendCounterMetric(null, PscMetrics.PSC_CONSUMER_RETRIES_REACHED_LIMIT_METRIC, consumerInternalConfiguration)
            );
        }

        pscConsumer.close();
    }

    /**
     * Verifies that custom auto resolution configuration takes effect and overrides the default.
     *
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testAutoResolutionConfiguration() throws ConfigurationException, ConsumerException, ProducerException, IOException {
        // default configs
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(pscConsumerConfiguration);
        PscConfigurationInternal pscConfigurationInternal = PscConsumerUtils.getPscConfigurationInternal(pscConsumer);
        assertTrue(pscConfigurationInternal.isAutoResolutionEnabled());
        assertEquals(5, pscConfigurationInternal.getAutoResolutionRetryCount());
        pscConsumer.close();

        // disabled auto resolution
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, false);
        pscConsumer = new PscConsumer<>(pscConsumerConfiguration);
        pscConfigurationInternal = PscConsumerUtils.getPscConfigurationInternal(pscConsumer);
        assertFalse(pscConfigurationInternal.isAutoResolutionEnabled());
        pscConsumer.close();

        // auto resolution with custom retry count
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, true);
        pscConsumerConfiguration.setProperty(PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, 105);
        pscConsumer = new PscConsumer<>(pscConsumerConfiguration);
        pscConfigurationInternal = PscConsumerUtils.getPscConfigurationInternal(pscConsumer);
        assertTrue(pscConfigurationInternal.isAutoResolutionEnabled());
        assertEquals(105, pscConfigurationInternal.getAutoResolutionRetryCount());
        pscConsumer.close();

        // auto resolution with custom retry count overriding default KafkaProducer retries
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, true);
        pscProducerConfiguration.setProperty(PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, 105);
        PscProducer<byte[], byte[]> pscProducer = new PscProducer<>(pscProducerConfiguration);
        pscProducer.getPartitions(topicUriStr1);
        pscConfigurationInternal = PscProducerUtils.getPscConfigurationInternal(pscProducer);
        assertTrue(pscConfigurationInternal.isAutoResolutionEnabled());
        assertEquals(105, pscConfigurationInternal.getAutoResolutionRetryCount());
        assertEquals(1, PscProducerUtils.getBackendProducersOf(pscProducer).size());
        assertEquals(
                100,
                PscProducerUtils.getBackendProducersOf(pscProducer).stream().iterator().next().getConfiguration().getInt("retries")
        );
        pscProducer.close();
    }

    /**
     * Verifies that disabled auto resolution does not lead to backend producer send retries. In this case, the backend
     * producer runs into a timeout exception since the topic does not initially exist on the cluster. Once the topic is
     * created a retry eventually sends the message.
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testDisabledAutoResolutionOnSendWithNonExistingTopic() throws Exception {
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "false");
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_BATCH_DURATION_MAX_MS, "0");
        // use a large enough retry count to make sure topic is created in time before retries are exhausted.
        pscProducerConfiguration.setProperty(PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, "20");
        // use a low max.block.ms so the internal retries end quickly.
        pscProducerConfiguration.setProperty("psc.producer.max.block.ms", 3000);
        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(pscProducerConfiguration);

        String nonExistingTopic = topic2 + "-not-there";
        String nonExistingTopicUri = topicUriStr2 + "-not-there";

        PscProducerMessage<Integer, Integer> pscProducerMessage = new PscProducerMessage<>(nonExistingTopicUri, 0, 0);
        Future<MessageId> future = pscProducer.send(pscProducerMessage);

        try {
            future.get();
            fail("Expected a timeout exception due to non-existing topic.");
        } catch (Exception exception) {
            assertEquals(ExecutionException.class, exception.getClass());
            assertEquals(TimeoutException.class, exception.getCause().getClass());
        }

        TopicUri nonExistingTopicUriValidated = TopicUri.validate(nonExistingTopicUri);

        long totalProducerRetries = PscMetricRegistryManager.getInstance()
                .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_RETRIES_REACHED_LIMIT_METRIC, producerInternalConfiguration);

        assertEquals(0, totalProducerRetries);
        assertEquals(
                0,
                PscMetricRegistryManager.getInstance()
                        .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_FAILURE + "." + TimeoutException.class.getName(), producerInternalConfiguration)
        );
        assertEquals(
                0,
                PscMetricRegistryManager.getInstance()
                        .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + TimeoutException.class.getName(), producerInternalConfiguration)
        );
        pscProducer.close();
    }

    /**
     * Verifies that auto resolution works as expected when a producer send call leads to an error. In this case, the
     * backend producer runs into a timeout exception since the topic does not initially exist on the cluster. Once the
     * topic is created a retry eventually sends the message.
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testAutoResolutionOnSendWithNonExistingTopic() throws Exception {
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "true");
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_BATCH_DURATION_MAX_MS, "0");
        // use a large enough retry count to make sure topic is created in time before retries are exhausted.
        pscProducerConfiguration.setProperty(PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, "20");
        // use a low max.block.ms so the internal retries end quickly.
        pscProducerConfiguration.setProperty("psc.producer.max.block.ms", 3000);
        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(pscProducerConfiguration);

        String nonExistingTopic = topic2 + "-not-there";
        String nonExistingTopicUri = topicUriStr2 + "-not-there";

        new Thread(() -> {
            try {
                logger.info("Topic creation thread (id: {}) is pausing before creating the topic.", Thread.currentThread().getId());
                Thread.sleep(10000);
                PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, nonExistingTopic, 1);
                logger.info("Topic {} was created.", nonExistingTopic);
            } catch (InterruptedException e) {
                Assertions.fail("Could not pause the topic creation thread.");
            }
        }).start();

        PscProducerMessage<Integer, Integer> pscProducerMessage = new PscProducerMessage<>(nonExistingTopicUri, 0, 0);
        Future<MessageId> future = pscProducer.send(pscProducerMessage);

        while (!future.isDone())
            Thread.sleep(1000);

        TopicUri nonExistingTopicUriValidated = TopicUri.validate(nonExistingTopicUri);

        long totalProducerRetries = PscMetricRegistryManager.getInstance()
                .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_RETRIES_METRIC, producerInternalConfiguration);

        assertTrue(totalProducerRetries > 0);
        assertEquals(
                totalProducerRetries - 1,
                PscMetricRegistryManager.getInstance()
                        .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_FAILURE + "." + TimeoutException.class.getName(), producerInternalConfiguration)
        );
        assertEquals(
                1,
                PscMetricRegistryManager.getInstance()
                        .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + TimeoutException.class.getName(), producerInternalConfiguration)
        );
        pscProducer.close();

        PscConsumer<Integer, Integer> pscConsumer = new PscConsumer<>(pscConsumerConfiguration);
        TopicUriPartition topicUriPartition = new TopicUriPartition(nonExistingTopicUri , 0);
        assertEquals(
                1,
                pscConsumer.endOffsets(Collections.singleton(topicUriPartition)).get(topicUriPartition)
        );
        pscConsumer.close();
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource, nonExistingTopic);
    }

    /**
     * Verifies that auto resolution quits after configured retries when a producer send call leads to an error. In this
     * case, the backend producer runs into a timeout exception since the topic does not initially exist on the cluster.
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testUnsuccessfulAutoResolutionOnSendWithNonExistingTopic() throws Exception {
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "true");
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_BATCH_DURATION_MAX_MS, "0");
        // use a large enough retry count to make sure topic is created in time before retries are exhausted.
        pscProducerConfiguration.setProperty(PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, "4");
        // use a low max.block.ms so the internal retries end quickly.
        pscProducerConfiguration.setProperty("psc.producer.max.block.ms", 3000);
        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(pscProducerConfiguration);

        String nonExistingTopicUri = topicUriStr2 + "-not-there";

        PscProducerMessage<Integer, Integer> pscProducerMessage = new PscProducerMessage<>(nonExistingTopicUri, 0, 0);
        Future<MessageId> future = pscProducer.send(pscProducerMessage);

        while (!future.isDone())
            Thread.sleep(1000);

        TopicUri nonExistingTopicUriValidated = TopicUri.validate(nonExistingTopicUri);

        PscConfigurationInternal pscConfigurationInternal = PscProducerUtils.getPscConfigurationInternal(pscProducer);

        long totalProducerRetries = PscMetricRegistryManager.getInstance()
                .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_RETRIES_METRIC, producerInternalConfiguration);

        assertEquals(pscConfigurationInternal.getAutoResolutionRetryCount(), totalProducerRetries);
        assertEquals(
                totalProducerRetries,
                PscMetricRegistryManager.getInstance()
                        .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_FAILURE + "." + TimeoutException.class.getName(), producerInternalConfiguration)
        );
        assertEquals(
                0,
                PscMetricRegistryManager.getInstance()
                        .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + TimeoutException.class.getName(), producerInternalConfiguration)
        );
        assertEquals(
                1,
                PscMetricRegistryManager.getInstance()
                        .getBackendCounterMetric(nonExistingTopicUriValidated, PscMetrics.PSC_PRODUCER_RETRIES_REACHED_LIMIT_METRIC, producerInternalConfiguration)
        );
        pscProducer.close();
    }

    /**
     * Verifies that auto resolution works on consumer commit when the exception returned to the callback is not null
     */
    @Test
    public void testAutoResolutionOnCommit() throws Exception {

        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(pscProducerConfiguration);
        PscProducerMessage<Integer, Integer> pscProducerMessage0 = new PscProducerMessage<>(topicUriStr2, 0, 0);
        PscProducerMessage<Integer, Integer> pscProducerMessage1 = new PscProducerMessage<>(topicUriStr2, 1, 1);
        PscProducerMessage<Integer, Integer> pscProducerMessage2 = new PscProducerMessage<>(topicUriStr2, 2, 2);
        PscProducerMessage<Integer, Integer> pscProducerMessage3 = new PscProducerMessage<>(topicUriStr2, 3, 3);


        pscProducer.send(pscProducerMessage0);
        pscProducer.send(pscProducerMessage1);

        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConsumerConfiguration.setProperty(PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, 10);
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(pscConsumerConfiguration);
        assertTrue(PscConsumerUtils.getPscConfigurationInternal(pscConsumer).isAutoResolutionEnabled());

        TopicUriPartition tup = new TopicUriPartition(topicUriStr2, 0);

        pscConsumer.subscribe(Collections.singletonList(topicUriStr2));
        PscConsumerPollMessageIterator<byte[], byte[]> messages = pscConsumer.poll();
        while (!messages.hasNext()) {
            messages = pscConsumer.poll();
        }

        // ensure nothing is committed yet
        assertNull(pscConsumer.committed(tup));

        // stop broker
        sharedKafkaTestResource.getKafkaBrokers().asList().get(0).stop();

        AtomicReference<Exception> ex = new AtomicReference<>();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

        // schedule start broker with delay
        executor.schedule(new Runnable() {

            @Override
            public void run() {
                try {
                    sharedKafkaTestResource.getKafkaBrokers().asList().get(0).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 15, TimeUnit.SECONDS);

        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {
            ex.set(exception);
        });
        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {
            ex.set(exception);
        });

        // assert committed offset is as expected
        assertNull(ex.get());
        assertNotNull(pscConsumer.committed(tup));
        assertEquals(2L, pscConsumer.committed(tup).getOffset());

        // send more messages
        pscProducer.send(pscProducerMessage2);
        pscProducer.send(pscProducerMessage3);

        PscConsumerPollMessageIterator<byte[], byte[]> messages2 = pscConsumer.poll();
        while (!messages2.hasNext()) {
            messages2 = pscConsumer.poll();
        }

        // ensure no further commits yet
        assertEquals(2L, pscConsumer.committed(tup).getOffset());

        // commit another time, should succeed
        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {
            ex.set(exception);
        });

        // ensure second commit succeeded
        assertEquals(4L, pscConsumer.committed(tup).getOffset());

        assertTrue(PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                KafkaTopicUri.validate(topicUriStr2), PscMetrics.PSC_CONSUMER_RETRIES_METRIC, consumerInternalConfiguration) > 0);
        assertEquals(1, PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                KafkaTopicUri.validate(topicUriStr2), PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + RetriableCommitFailedException.class.getName(), consumerInternalConfiguration));

        pscConsumer.close();
        pscProducer.close();
    }

    /**
     * Verifies that disabled auto resolution works on consumer commit
     */
    @Test
    public void testDisabledAutoResolutionOnCommit() throws Exception {

        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(pscProducerConfiguration);
        PscProducerMessage<Integer, Integer> pscProducerMessage0 = new PscProducerMessage<>(topicUriStr2, 0, 0);
        PscProducerMessage<Integer, Integer> pscProducerMessage1 = new PscProducerMessage<>(topicUriStr2, 1, 1);
        PscProducerMessage<Integer, Integer> pscProducerMessage2 = new PscProducerMessage<>(topicUriStr2, 2, 2);
        PscProducerMessage<Integer, Integer> pscProducerMessage3 = new PscProducerMessage<>(topicUriStr2, 3, 3);


        pscProducer.send(pscProducerMessage0);
        pscProducer.send(pscProducerMessage1);

        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConsumerConfiguration.setProperty(PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, 100);
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, false);
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(pscConsumerConfiguration);
        assertFalse(PscConsumerUtils.getPscConfigurationInternal(pscConsumer).isAutoResolutionEnabled());

        TopicUriPartition tup = new TopicUriPartition(topicUriStr2, 0);

        pscConsumer.subscribe(Collections.singletonList(topicUriStr2));
        PscConsumerPollMessageIterator<byte[], byte[]> messages = pscConsumer.poll();
        while (!messages.hasNext()) {
            messages = pscConsumer.poll();
        }

        // ensure nothing is committed yet
        assertNull(pscConsumer.committed(tup));

        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {;
        });
        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {
        });

        // assert committed offset is as expected
        assertNotNull(pscConsumer.committed(tup));
        assertEquals(2L, pscConsumer.committed(tup).getOffset());

        // send more messages
        pscProducer.send(pscProducerMessage2);
        pscProducer.send(pscProducerMessage3);

        PscConsumerPollMessageIterator<byte[], byte[]> messages2 = pscConsumer.poll();
        while (!messages2.hasNext()) {
            messages2 = pscConsumer.poll();
        }

        // ensure no further commits yet
        assertEquals(2L, pscConsumer.committed(tup).getOffset());

        // commit another time, should succeed
        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {
        });

        // ensure second commit succeeded
        assertEquals(4L, pscConsumer.committed(tup).getOffset());

        // ensure no auto resolution was done
        assertEquals(0, PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                KafkaTopicUri.validate(topicUriStr2), PscMetrics.PSC_CONSUMER_RETRIES_METRIC, consumerInternalConfiguration));
        assertEquals(0, PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                KafkaTopicUri.validate(topicUriStr2), PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_FAILURE + "." + RetriableCommitFailedException.class.getName(), consumerInternalConfiguration));
        assertEquals(0, PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                KafkaTopicUri.validate(topicUriStr2), PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + RetriableCommitFailedException.class.getName(), consumerInternalConfiguration));

        pscConsumer.close();
        pscProducer.close();

    }

    /**
     * Verifies that in the sad path when auto resolution retry limit is reached, the commitCallback.onCompletion() API is still called
     * and client reset is done
     */
    @Test
    public void testAutoResolutionOnCommitSadPath() throws Exception {

        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(pscProducerConfiguration);
        PscProducerMessage<Integer, Integer> pscProducerMessage0 = new PscProducerMessage<>(topicUriStr2, 0, 0);
        PscProducerMessage<Integer, Integer> pscProducerMessage1 = new PscProducerMessage<>(topicUriStr2, 1, 1);


        pscProducer.send(pscProducerMessage0);
        pscProducer.send(pscProducerMessage1);

        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        pscConsumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConsumerConfiguration.setProperty(PscConfiguration.PCS_AUTO_RESOLUTION_RETRY_COUNT, 5);
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(pscConsumerConfiguration);
        assertTrue(PscConsumerUtils.getPscConfigurationInternal(pscConsumer).isAutoResolutionEnabled());

        TopicUriPartition tup = new TopicUriPartition(topicUriStr2, 0);

        pscConsumer.subscribe(Collections.singletonList(topicUriStr2));
        PscConsumerPollMessageIterator<byte[], byte[]> messages = pscConsumer.poll();
        while (!messages.hasNext()) {
            messages = pscConsumer.poll();
        }

        // ensure nothing is committed yet
        assertNull(pscConsumer.committed(tup));

        // stop broker
        sharedKafkaTestResource.getKafkaBrokers().asList().get(0).stop();

        AtomicReference<Exception> ex = new AtomicReference<>();

        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {
            ex.set(exception);
        });
        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {
            ex.set(exception);
        });

        // assert commit callback was successfully executed
        assertNotNull(ex.get());
        assertEquals(RetriableCommitFailedException.class, ex.get().getClass());

        assertTrue(PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                KafkaTopicUri.validate(topicUriStr2), PscMetrics.PSC_CONSUMER_RETRIES_METRIC, consumerInternalConfiguration) > 0);
        assertEquals(0, PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                KafkaTopicUri.validate(topicUriStr2), PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + RetriableCommitFailedException.class.getName(), consumerInternalConfiguration));
        assertEquals(1, PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                KafkaTopicUri.validate(topicUriStr2), PscMetrics.PSC_CONSUMER_RETRIES_REACHED_LIMIT_METRIC, consumerInternalConfiguration));

        // reset should be done on next invocation
        assertEquals(0, PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                null, PscMetrics.PSC_CONSUMER_RESETS_METRIC, consumerInternalConfiguration));

        // simulate commit interval
        Thread.sleep(5000);

        // next commit should reset backend client
        pscConsumer.commitAsync((Map<TopicUriPartition, MessageId> offsets, Exception exception) -> {
            ex.set(exception);
        });
        assertEquals(1, PscMetricRegistryManager.getInstance().getBackendCounterMetric(
                null, PscMetrics.PSC_CONSUMER_RESETS_METRIC, consumerInternalConfiguration));

        // start broker again to allow tearDown()
        sharedKafkaTestResource.getKafkaBrokers().asList().get(0).start();

        Thread.sleep(3000);

        pscConsumer.close();
        pscProducer.close();
    }
}
