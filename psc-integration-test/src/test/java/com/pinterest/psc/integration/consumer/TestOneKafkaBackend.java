package com.pinterest.psc.integration.consumer;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscBackendConsumer;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.consumer.PscConsumerUtils;
import com.pinterest.psc.consumer.creation.PscKafkaConsumerCreator;
import com.pinterest.psc.consumer.listener.MessageCounterListener;
import com.pinterest.psc.consumer.listener.SimpleConsumerRebalanceListener;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.producer.SerializerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.integration.IdentityInterceptor;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricTag;
import com.pinterest.psc.metrics.PscMetricsUtil;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.pinterest.psc.utils.Utils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.vavr.control.Either;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestOneKafkaBackend {
    private static final PscLogger logger = PscLogger.getLogger(TestOneKafkaBackend.class);
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokers(1)
            .withBrokerProperty("auto.create.topics.enable", "false");
    private static AdminClient adminClient;
    private static final int TEST_TIMEOUT_SECONDS = 120;
    private static final int PSC_CONSUME_TIMEOUT_MS = 7500;
    private static final PscConfiguration pscConfiguration = new PscConfiguration();
    private static String baseClientId;
    private static String baseGroupId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 12;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 24;
    private static final String topic3 = "topic3";
    private static final int partitions3 = 1;
    private KafkaCluster kafkaCluster;
    private String topicRnStr1, topicUriStr1, topicUriStr2, topicUriStr3;

    @BeforeAll
    public static void prepare() {
        adminClient = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient();
    }

    @AfterAll
    public static void shutdown() {
        adminClient.close();
    }

    /**
     * Initializes a Kafka cluster that is commonly used by all tests, and creates a single topic on it.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeEach
    public void setup() throws IOException, InterruptedException {
        baseClientId = this.getClass().getSimpleName() + "-psc-consumer-client";
        baseGroupId = this.getClass().getSimpleName() + "-psc-consumer-group";
        pscConfiguration.clear();
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, baseGroupId + "-" + UUID.randomUUID());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
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
        topicUriStr2 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), topic2);
        topicUriStr3 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), topic3);

        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic1, partitions1);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic2, partitions2);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic3, partitions3);
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
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource, topic3);
    }

    /**
     * Verifies that PSC configurations are properly mapped to backend (Kafka) configs.
     *
     * @throws SerializerException
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConvertedConfigNamesDontHonorOldNames() throws SerializerException, ConfigurationException, ConsumerException {
        int messageCount = 100;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic3);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        // PSC default for PSC_CONSUMER_BUFFER_RECEIVE_BYTES is 1 MB
        pscConfiguration.setProperty("psc.consumer.receive.buffer.bytes", "1024");
        // PSC default for PSC_CONSUMER_OFFSET_AUTO_RESET is null (to enforce the default value from backend)
        pscConfiguration.setProperty("psc.consumer.auto.offset.reset", "earliest");

        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUriStr3, 0);
        pscConsumer.assign(Collections.singleton(topicUriPartition));

        PscConfiguration backendConsumerConfig = PscConsumerUtils.getBackendConsumerConfiguration(pscConsumer).get(topicUriStr3);
        assertEquals(1048576, backendConsumerConfig.getLong("receive.buffer.bytes"));
        assertNull(backendConsumerConfig.getString("auto.offset.reset"));
        assertEquals(
                (long) pscConsumer.endOffsets(Collections.singleton(topicUriPartition)).get(topicUriPartition),
                pscConsumer.position(topicUriPartition)
        );
        pscConsumer.close();
    }

    /**
     * Verifies deserializer objects can be used for consumer key/value deserializer configuration.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testDeserializerObjectsInConfig() throws SerializerException, InterruptedException {
        int messageCount = 100;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, new StringDeserializer());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, new StringDeserializer());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(partitions1, pscConsumerRunner.getAssignment().size());
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount, messages.size());

        Set<Integer> idsConsumed = new HashSet<>();
        for (PscConsumerMessage<String, String> message : messages) {
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            idsConsumed.add(key);
        }
        assertEquals(messageCount, idsConsumed.size());
        int min = idsConsumed.stream().reduce(Integer::min).get();
        int max = idsConsumed.stream().reduce(Integer::max).get();
        assertEquals(0, min);
        assertEquals(messageCount - 1, max);
    }

    /**
     * Verifies that the correct partition count is returned for the given topic URI.
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws WakeupException
     */
    @Test
    public void testGetPartitions() throws ConsumerException, ConfigurationException, WakeupException {
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(pscConfiguration);
        assertEquals(partitions1, pscConsumer.getPartitions(topicUriStr1).size());
        pscConsumer.close();
    }

    /**
     * Uses KafkaProducer to generate a specific number of message to the topic. Then a PscConsumer subscribes and
     * consumes from the topic from the beginning. The test verifies that the same number of messages are read by the
     * consumer and the messages consumed are as expected (content-wise).
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSubscribeConsumption() throws SerializerException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(partitions1, pscConsumerRunner.getAssignment().size());
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount, messages.size());

        Set<Integer> idsConsumed = new HashSet<>();
        for (PscConsumerMessage<String, String> message : messages) {
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            idsConsumed.add(key);
        }
        assertEquals(messageCount, idsConsumed.size());
        int min = idsConsumed.stream().reduce(Integer::min).get();
        int max = idsConsumed.stream().reduce(Integer::max).get();
        assertEquals(0, min);
        assertEquals(messageCount - 1, max);
    }

    /**
     * Uses KafkaProducer to generate a specific number of message to a single partition of the topic. Then that
     * partition is assigned to a PscConsumer. The consumer then consumes from the partition from the beginning.
     * The test verifies that the same number of messages are read by the consumer and the messages consumed are
     * as expected (content-wise).
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testAssignConsumption() throws SerializerException, InterruptedException {
        int messageCount = 1000;
        int targetPartition = 0;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUriStr1, targetPartition);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(Collections.singleton(topicUriPartition)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount, messages.size());

        int index = 0;
        for (PscConsumerMessage<String, String> message : messages) {
            assertEquals(targetPartition, message.getMessageId().getTopicUriPartition().getPartition());
            assertEquals(index, message.getMessageId().getOffset());
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            assertEquals(index++, key);
        }
        assertEquals(messageCount, index);
    }

    /**
     * Tests that the SimpleConsumerRebalanceListener class is correctly passed upon
     * subscription with 2 consumers and that the number of partitions assigned and revoked
     * is correct after the following scenario using a topic with 12 partitions:
     * 1. Subscribe first consumer in group
     *  [first_assigned: 12, first_revoked: 0, second_assigned: -1, second_revoked: -1]
     * 2. Subscribe second consumer in group
     *  [first_assigned: 6, first_revoked: 12, second_assigned: 6, second_revoked: 0]
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSubscribtionWithRebalanceListener()
        throws SerializerException, ConfigurationException,
               ConsumerException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster,
            topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, 1);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET,
            PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER,
            StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER,
            StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID,
            this.getClass().getSimpleName() + "-psc-consumer-group");

        PscConsumer<String, String> pscConsumer1 = new PscConsumer<>(pscConfiguration);
        PscConsumer<String, String> pscConsumer2 = new PscConsumer<>(pscConfiguration);

        pscConsumer1.subscribe(Collections.singleton(topicUriStr1),
            new SimpleConsumerRebalanceListener());
        // Do initial consumption
        while (pscConsumer1.assignment().size() <= 0) {
            pscConsumer1.poll();
        }
        assertEquals(12,
            ((SimpleConsumerRebalanceListener) pscConsumer1.getRebalanceListener()).getAssigned());
        assertEquals(-1,
            ((SimpleConsumerRebalanceListener) pscConsumer1.getRebalanceListener()).getRevoked());
        // add second consumer to group
        pscConsumer2.subscribe(Collections.singleton(topicUriStr1),
            new SimpleConsumerRebalanceListener());
        while (pscConsumer2.assignment().size() <= 0) {
            pscConsumer1.poll();
            pscConsumer2.poll();
        }
        assertEquals(6,
            ((SimpleConsumerRebalanceListener) pscConsumer1.getRebalanceListener()).getAssigned());
        assertEquals(6,
            ((SimpleConsumerRebalanceListener) pscConsumer2.getRebalanceListener()).getAssigned());
        assertEquals(12,
            ((SimpleConsumerRebalanceListener) pscConsumer1.getRebalanceListener()).getRevoked());
        assertEquals(-1,
            ((SimpleConsumerRebalanceListener) pscConsumer2.getRebalanceListener()).getRevoked());
        pscConsumer1.unsubscribe();
        pscConsumer2.unsubscribe();
        pscConsumer1.close();
        pscConsumer2.close();
    }

    /**
     * Verifies that a configured listener works as expected and processes every message. Also, it makes sure that
     * when listener is configured, using PSC consumer poll() is disallowed.
     *
     * @throws SerializerException
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testListener() throws SerializerException, ConsumerException, ConfigurationException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_MESSAGE_LISTENER, MessageCounterListener.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        assertThrows(ConsumerException.class, pscConsumer::poll);
        Thread.sleep(5000);
        pscConsumer.unsubscribe();
        pscConsumer.close();
        assertEquals(messageCount, ((MessageCounterListener) pscConsumer.getListener()).getHandleCallCounter());
    }

    /**
     * Verifies that a configured listener works as expected and processes every message when configured using a
     * message listener object. Also, it makes sure that when listener is configured, using PSC consumer poll() is
     * disallowed.
     *
     * @throws SerializerException
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testListenerWithListenerObjectInConfig()
            throws SerializerException, ConsumerException, ConfigurationException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_MESSAGE_LISTENER, new MessageCounterListener<>());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        assertThrows(ConsumerException.class, pscConsumer::poll);
        Thread.sleep(5000);
        pscConsumer.unsubscribe();
        pscConsumer.close();
        assertEquals(messageCount, ((MessageCounterListener) pscConsumer.getListener()).getHandleCallCounter());
    }

    /**
     * Verifies that a configured listener stops listening once the consumer unsubscribes.
     *
     * @throws SerializerException
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testListenerStopsWithUnsubscribe() throws SerializerException, ConsumerException, ConfigurationException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_MESSAGE_LISTENER, MessageCounterListener.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, "10");
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        assertThrows(ConsumerException.class, pscConsumer::poll);
        Thread.sleep(100);
        pscConsumer.unsubscribe();
        int count = ((MessageCounterListener) pscConsumer.getListener()).getHandleCallCounter();
        assertTrue(count < messageCount);
        for (int i = 0; i < 10; ++i) {
            Thread.sleep(100);
            assertEquals(count, ((MessageCounterListener) pscConsumer.getListener()).getHandleCallCounter());
        }
        pscConsumer.close();
    }

    /**
     * Verifies that the consumer group behavior works as expected. This is done by launching multiple consumers that
     * are subscribed to the same topic and confirming that each consumer is assigned an exclusive subset of all
     * partitions.
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    //TODO: Flaky Test
    @Disabled
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConsumerGroupSubscription() throws SerializerException, InterruptedException, ExecutionException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");

        List<PscConsumerRunner<String, String>> pscConsumerRunners = new ArrayList<>();
        // Let's generate random group size in [2, partitions1]
        int consumerCount = new Random().nextInt(partitions1 - 1) + 2;
        for (int i = 0; i < consumerCount; ++i) {
            pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
            PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                    pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), 5 * PSC_CONSUME_TIMEOUT_MS
            );
            pscConsumerRunners.add(pscConsumerRunner);
            pscConsumerRunner.kickoff();
        }

        Thread.sleep(3 * PSC_CONSUME_TIMEOUT_MS);
        String groupId = pscConfiguration.getString(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertEquals(
                consumerCount,
                adminClient.describeConsumerGroups(Collections.singleton(groupId)).describedGroups().get(groupId)
                        .get().members().size()
        );

        // wait for all threads to run to completion
        for (PscConsumerRunner<String, String> pscConsumerRunner : pscConsumerRunners)
            pscConsumerRunner.waitForCompletion();

        // verify that each consumer was assigned exclusive partitions and all partitions were assigned.
        Set<TopicUriPartition> assignedPartitions = new HashSet<>();
        List<PscConsumerMessage<String, String>> allMessages = new ArrayList<>();
        for (PscConsumerRunner<String, String> pscConsumerRunner : pscConsumerRunners) {
            assertFalse(pscConsumerRunner.getAssignment().isEmpty());
            for (TopicUriPartition topicUriPartition : pscConsumerRunner.getAssignment()) {
                assertFalse(String.format("Unexpected topic URI partition: %s", topicUriPartition),
                        assignedPartitions.contains(topicUriPartition)
                );
                assertEquals(topicUriStr1, topicUriPartition.getTopicUriAsString());
                assertTrue(0 <= topicUriPartition.getPartition() && topicUriPartition.getPartition() < partitions1);
                assignedPartitions.add(topicUriPartition);
            }
            assertFalse(pscConsumerRunner.getMessages().isEmpty());
            allMessages.addAll(pscConsumerRunner.getMessages());
        }
        assertEquals(partitions1, assignedPartitions.size());
        assertEquals(messageCount, allMessages.size());
    }

    /**
     * Verifies that seeking to a message id works as expected when PSC consumer is using the subscribe() API. This test
     * first attempts at seeking to a message id that consumer is subscribed to and verifies the thrown exception. Then
     * it tries a valid message id and verifies that the seek takes place.
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws TopicUriSyntaxException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekByMessageIdWithSubscribeErrorScenarios() throws SerializerException, InterruptedException, TopicUriSyntaxException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount, messages.size());

        // an invalid messageId
        TopicUri badTopicUri = TopicUri.validate(topicUriStr1 + "bad");
        MessageId badMessageId = new MessageId(new TopicUriPartition(badTopicUri.getTopicUriAsString(), 0), 1000);
        pscConsumerRunner.setMessageIdsToSeek(Collections.singleton(badMessageId));
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertNotNull(pscConsumerRunner.getException());
        assertEquals(ConsumerException.class, pscConsumerRunner.getException().getClass());
    }

    /**
     * Verifies that seeking to a set of message id works as expected when PSC consumer is using the subscribe() API.
     * It tries to seek to a random number of messaged (capped by topic partition count) and verify that the pointer
     * on those partitions after seek is pointing to the correct message.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekByMessageIdsWithSubscribe() throws SerializerException, InterruptedException {
        int messageCount = 10000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount, messages.size());

        // pick random message ids from non-duplicate partitions to seek to
        Random random = new Random();
        int messageIdCountToSeek = 1 + random.nextInt(partitions1);
        logger.info("Testing seek by subscribe with {} message ids", messageIdCountToSeek);
        Set<MessageId> messageIds = PscTestUtils.getRandomMessageIdsWithDistinctPartitions(messages, messageIdCountToSeek);

        // seek and verify that the first message read from each seeked partition is the one we expect
        pscConsumerRunner.setMessageIdsToSeek(messageIds);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messages = pscConsumerRunner.getMessages();
        assertTrue(
                "Did not consume all expected messages after seeking to a set of message ids.",
                PscTestUtils.verifyMessagesStartWithMessageIds(messages, messageIds)
        );
    }

    /**
     * Verifies that seeking to a message id works as expected when PSC consumer is using the assign() API. This test
     * first attempts at seeking to message ids not assigned to the consumer and verifies the thrown exception. Then
     * it tries a valid message id and verifies that the seek takes place properly.
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws TopicUriSyntaxException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekByMessageIdWithAssignErrorScenarios() throws SerializerException, InterruptedException,
            TopicUriSyntaxException {
        int messageCount = 1000;
        int targetPartition = 0;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUriStr1, targetPartition);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(Collections.singleton(topicUriPartition)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount, messages.size());

        // an invalid messageId
        TopicUri badTopicUri = TopicUri.validate(topicUriStr1 + "bad");
        MessageId badMessageId = new MessageId(new TopicUriPartition(badTopicUri.getTopicUriAsString(), 0), 1000);
        pscConsumerRunner.setMessageIdsToSeek(Collections.singleton(badMessageId));
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertNotNull(pscConsumerRunner.getException());
        assertEquals(ConsumerException.class, pscConsumerRunner.getException().getClass());

        // a partition not assigned to consumer
        TopicUri topicUri = TopicUri.validate(topicUriStr1);
        badMessageId = new MessageId(new TopicUriPartition(topicUri.getTopicUriAsString(), (targetPartition + 1) % partitions1), 1000);
        pscConsumerRunner.setMessageIdsToSeek(Collections.singleton(badMessageId));
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertNotNull(pscConsumerRunner.getException());
        assertEquals(ConsumerException.class, pscConsumerRunner.getException().getClass());
    }

    /**
     * Verifies that seeking to a message id works as expected when PSC consumer is using the assign() API. This test
     * first attempts at seeking to message ids not assigned to the consumer and verifies the thrown exception. Then
     * it tries random valid message ids and verifies that the pointer on those partitions after seek is pointing to
     * the correct message.
     *
     * @throws Exception
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekByMessageIdsWithAssign() throws Exception {
        int messageCount = 10000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        // assign random partitions to consumer
        Random random = new Random();
        Set<Integer> assignedPartitions
                = Utils.getRandomNumbers(0, partitions1 - 1, 1 + random.nextInt(partitions1));
        Set<TopicUriPartition> topicUriPartitions = new HashSet<>();
        assignedPartitions.forEach(partition -> topicUriPartitions.add(new TopicUriPartition(topicUriStr1, partition)));
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(topicUriPartitions), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();

        // pick random message ids from non-duplicate partitions to seek to
        int messageIdCountToSeek = 1 + random.nextInt(assignedPartitions.size());
        logger.info("Testing seek by assign with {} message ids and {} assigned partitions",
                messageIdCountToSeek, assignedPartitions.size());
        Set<MessageId> messageIds = PscTestUtils.getRandomMessageIdsWithDistinctPartitions(messages, messageIdCountToSeek);

        // seek and verify that the first message read from each seeked partition is the one we expect
        pscConsumerRunner.setMessageIdsToSeek(messageIds);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messages = pscConsumerRunner.getMessages();
        assertTrue(
                "Did not consume all expected messages after seeking to a set of message ids.",
                PscTestUtils.verifyMessagesStartWithMessageIds(messages, messageIds)
        );
    }

    /**
     * Verifies that seeking to timestamps works as expected when PSC consumer is using the subscribe() API. This test
     * tries seeking to a specific timestamp for a couple of partitions and verifies that the pointer on those
     * partitions after seek is pointing to the correct message.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(2 * TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekByTimestampWithSubscribe() throws SerializerException, InterruptedException {
        int messageCount = 10000;
        long timestamp1 = System.currentTimeMillis();
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        Thread.sleep(10000);
        long timestamp2 = System.currentTimeMillis();
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        Thread.sleep(10000);
        long timestamp3 = System.currentTimeMillis();
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        long timestamp4 = System.currentTimeMillis();

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, 1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, 2);
        Map<TopicUriPartition, Long> partitionTimestamps = new HashMap<>();
        partitionTimestamps.put(topicUriPartition1, timestamp2); // min timestamp expected for messages from partition 1
        partitionTimestamps.put(topicUriPartition2, timestamp3); // min timestamp expected for messages from partition 2
        pscConsumerRunner.setPartitionTimestampsToSeek(partitionTimestamps);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();

        // find first message of each partition
        Map<Integer, PscConsumerMessage<String, String>> firstMessageByPartition = new HashMap<>();
        Iterator<PscConsumerMessage<String, String>> messageIterator = messages.iterator();
        PscConsumerMessage<String, String> nextMessage;
        while (firstMessageByPartition.size() < partitions1 && messageIterator.hasNext()) {
            nextMessage = messageIterator.next();
            if (!firstMessageByPartition.containsKey(nextMessage.getMessageId().getTopicUriPartition().getPartition()))
                firstMessageByPartition.put(nextMessage.getMessageId().getTopicUriPartition().getPartition(), nextMessage);
        }

        assertEquals(partitions1, firstMessageByPartition.size());

        // verify that
        // - the message from partition 1 has timestamp of > timestamp2 and < timestamp3
        // - the message from partition 2 has timestamp of > timestamp3 and < timestamp4
        // - every other message has timestamp of > timestamp1 and < timestamp2
        for (int i = 0; i < partitions1; ++i) {
            PscConsumerMessage<String, String> firstMessage = firstMessageByPartition.get(i);
            switch (i) {
                case 1:
                    assertTrue(firstMessage.getPublishTimestamp() > timestamp2);
                    assertTrue(firstMessage.getPublishTimestamp() < timestamp3);
                    break;
                case 2:
                    assertTrue(firstMessage.getPublishTimestamp() > timestamp3);
                    assertTrue(firstMessage.getPublishTimestamp() < timestamp4);
                    break;
                default:
                    assertTrue(firstMessage.getPublishTimestamp() > timestamp1);
                    assertTrue(firstMessage.getPublishTimestamp() < timestamp2);
            }
        }
    }

    /**
     * Verifies that with multiple PSC consumers that subscribe to the same topic URI, seeking one consumer to the
     * topic URI and a timestamps only moves the consumption pointer on the partitions that are assigned to that
     * consumer. Other consumers' pointers will not change by this seek.
     *
     * @throws SerializerException
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws WakeupException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekByTopicUriAndTimestampWithSubscribeAndMultipleConsumers() throws SerializerException, ConsumerException, ConfigurationException, WakeupException {
        int messageCount = 1000;
        long timestamp1 = System.currentTimeMillis();
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumer<String, String> pscConsumer1 = new PscConsumer<>(pscConfiguration);
        PscConsumer<String, String> pscConsumer2 = new PscConsumer<>(pscConfiguration);

        pscConsumer1.subscribe(Collections.singleton(topicUriStr1));
        pscConsumer2.subscribe(Collections.singleton(topicUriStr1));

        // let both consumers consume all existing messages
        AtomicInteger consumedMessageCount = new AtomicInteger(0);
        while (consumedMessageCount.get() < messageCount) {
            pscConsumer1.poll().forEachRemaining(message -> consumedMessageCount.incrementAndGet());
            pscConsumer2.poll().forEachRemaining(message -> consumedMessageCount.incrementAndGet());
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Set<TopicUriPartition> assignment1 = pscConsumer1.assignment();
        Set<TopicUriPartition> assignment2 = pscConsumer2.assignment();

        Map<TopicUriPartition, Long> currentPositions = new HashMap<>();
        for (TopicUriPartition topicUriPartition : assignment1) {
            assertFalse(currentPositions.containsKey(topicUriPartition));
            currentPositions.put(topicUriPartition, pscConsumer1.position(topicUriPartition));
        }
        for (TopicUriPartition topicUriPartition : assignment2) {
            assertFalse(currentPositions.containsKey(topicUriPartition));
            currentPositions.put(topicUriPartition, pscConsumer2.position(topicUriPartition));
        }

        // now call seek topic on one consumer only
        pscConsumer1.seekToTimestamp(topicUriStr1, timestamp1);

        // verify that only partitions assigned to that consumer are sought
        for (TopicUriPartition topicUriPartition : assignment1) {
            assertTrue(currentPositions.containsKey(topicUriPartition));
            assertTrue(pscConsumer1.position(topicUriPartition) < currentPositions.get(topicUriPartition));
        }
        for (TopicUriPartition topicUriPartition : assignment2) {
            assertTrue(currentPositions.containsKey(topicUriPartition));
            assertEquals(pscConsumer2.position(topicUriPartition), (long) currentPositions.get(topicUriPartition));
        }

        pscConsumer1.close();
        pscConsumer2.close();
    }

    /**
     * Verifies that seeking to timestamps works as expected when PSC consumer is using the assign() API. This test
     * tries seeking to a specific timestamp for one partition out of the two assigned, and verifies that the pointer
     * on those partitions after seek is pointing to the correct message.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekByTimestampWithAssign() throws SerializerException, InterruptedException {
        int messageCount = 10000;
        long timestamp1 = System.currentTimeMillis();
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        Thread.sleep(10000);
        long timestamp2 = System.currentTimeMillis();
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        long timestamp3 = System.currentTimeMillis();

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, 1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, 2);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(Sets.newHashSet(topicUriPartition1, topicUriPartition2)), PSC_CONSUME_TIMEOUT_MS
        );

        Map<TopicUriPartition, Long> partitionTimestamps = new HashMap<>();
        partitionTimestamps.put(topicUriPartition1, timestamp2); // min timestamp expected for messages from partition 1
        pscConsumerRunner.setPartitionTimestampsToSeek(partitionTimestamps);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();

        // find first message of each assigned partition
        Map<Integer, PscConsumerMessage<String, String>> firstMessageByPartition = new HashMap<>();
        Iterator<PscConsumerMessage<String, String>> messageIterator = messages.iterator();
        PscConsumerMessage<String, String> nextMessage;
        while (firstMessageByPartition.size() < 2 && messageIterator.hasNext()) {
            nextMessage = messageIterator.next();
            assertTrue(nextMessage.getMessageId().getTopicUriPartition().getPartition() == 1 ||
                    nextMessage.getMessageId().getTopicUriPartition().getPartition() == 2);
            if (!firstMessageByPartition.containsKey(nextMessage.getMessageId().getTopicUriPartition().getPartition()))
                firstMessageByPartition.put(nextMessage.getMessageId().getTopicUriPartition().getPartition(), nextMessage);
        }
        assertEquals(2, firstMessageByPartition.size());

        // verify that
        // - the message from partition 1 has timestamp of > timestamp2 and < timestamp3
        // - the message from partition 2 has timestamp of > timestamp1 and < timestamp2
        PscConsumerMessage<String, String> firstMessageP1 = firstMessageByPartition.get(1);
        assertNotNull(firstMessageP1);
        assertTrue(firstMessageP1.getPublishTimestamp() > timestamp2);
        assertTrue(firstMessageP1.getPublishTimestamp() < timestamp3);

        PscConsumerMessage<String, String> firstMessageP2 = firstMessageByPartition.get(2);
        assertNotNull(firstMessageP2);
        assertTrue(firstMessageP2.getPublishTimestamp() > timestamp1);
        assertTrue(firstMessageP2.getPublishTimestamp() < timestamp2);
    }

    /**
     * Verifies that seeking to offsets works as expected when PSC consumer is using the subscribe() API. This test
     * tries seeking to specific offsets for a few of partitions and verifies that the pointer on those partitions after
     * seek is pointing to the correct message.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekToOffsetWithSubscribe() throws SerializerException, InterruptedException {
        int messageCount = 10000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, 1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, 2);
        TopicUriPartition topicUriPartition3 = new TopicUriPartition(topicUriStr1, 3);
        Map<TopicUriPartition, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(topicUriPartition1, 10L);
        partitionOffsets.put(topicUriPartition2, 20L);
        partitionOffsets.put(topicUriPartition3, 30L);
        pscConsumerRunner.setPartitionOffsetsToSeek(partitionOffsets);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();

        // find first message of each partition
        Map<Integer, PscConsumerMessage<String, String>> firstMessageByPartition = new HashMap<>();
        Iterator<PscConsumerMessage<String, String>> messageIterator = messages.iterator();
        PscConsumerMessage<String, String> nextMessage;
        while (firstMessageByPartition.size() < partitions1 && messageIterator.hasNext()) {
            nextMessage = messageIterator.next();
            if (!firstMessageByPartition.containsKey(nextMessage.getMessageId().getTopicUriPartition().getPartition()))
                firstMessageByPartition.put(nextMessage.getMessageId().getTopicUriPartition().getPartition(), nextMessage);
        }

        assertEquals(partitions1, firstMessageByPartition.size());

        // verify that
        // - the message from partition 1 has offset 10
        // - the message from partition 2 has offset 20
        // - the message from partition 3 has offset 30
        // - every other message has offset 0
        for (int i = 0; i < partitions1; ++i) {
            PscConsumerMessage<String, String> firstMessage = firstMessageByPartition.get(i);
            switch (i) {
                case 1:
                    assertEquals(10L, firstMessage.getMessageId().getOffset());
                    break;
                case 2:
                    assertEquals(20L, firstMessage.getMessageId().getOffset());
                    break;
                case 3:
                    assertEquals(30L, firstMessage.getMessageId().getOffset());
                    break;
                default:
                    assertEquals(0L, firstMessage.getMessageId().getOffset());
            }
        }
    }

    /**
     * Verifies that seeking to offsets works as expected when PSC consumer is using the assign() API. This test
     * tries seeking to specific offsets for two partition out of the three assigned, and verifies that the pointer
     * on those partitions after seek is pointing to the correct message.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekToOffsetWithAssign() throws SerializerException, InterruptedException {
        int messageCount = 10000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, 1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, 2);
        TopicUriPartition topicUriPartition3 = new TopicUriPartition(topicUriStr1, 3);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration,
                Either.right(Sets.newHashSet(topicUriPartition1, topicUriPartition2, topicUriPartition3)),
                PSC_CONSUME_TIMEOUT_MS
        );

        Map<TopicUriPartition, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(topicUriPartition1, 10L);
        partitionOffsets.put(topicUriPartition2, 20L);
        pscConsumerRunner.setPartitionOffsetsToSeek(partitionOffsets);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();

        // find first message of each assigned partition
        Map<Integer, PscConsumerMessage<String, String>> firstMessageByPartition = new HashMap<>();
        Iterator<PscConsumerMessage<String, String>> messageIterator = messages.iterator();
        PscConsumerMessage<String, String> nextMessage;
        while (firstMessageByPartition.size() < 3 && messageIterator.hasNext()) {
            nextMessage = messageIterator.next();
            assertTrue(nextMessage.getMessageId().getTopicUriPartition().getPartition() == 1 ||
                    nextMessage.getMessageId().getTopicUriPartition().getPartition() == 2 ||
                    nextMessage.getMessageId().getTopicUriPartition().getPartition() == 3);
            if (!firstMessageByPartition.containsKey(nextMessage.getMessageId().getTopicUriPartition().getPartition()))
                firstMessageByPartition.put(nextMessage.getMessageId().getTopicUriPartition().getPartition(), nextMessage);
        }
        assertEquals(3, firstMessageByPartition.size());

        // verify that
        // - the message from partition 1 has offset 10
        // - the message from partition 2 has offset 20
        // - the message from partition 3 has offset 0
        PscConsumerMessage<String, String> firstMessageP1 = firstMessageByPartition.get(1);
        assertNotNull(firstMessageP1);
        assertEquals(10L, firstMessageP1.getMessageId().getOffset());

        PscConsumerMessage<String, String> firstMessageP2 = firstMessageByPartition.get(2);
        assertNotNull(firstMessageP2);
        assertEquals(20L, firstMessageP2.getMessageId().getOffset());

        PscConsumerMessage<String, String> firstMessageP3 = firstMessageByPartition.get(3);
        assertNotNull(firstMessageP3);
        assertEquals(0L, firstMessageP3.getMessageId().getOffset());
    }

    /**
     * Verifies that PSC consumer async commit works as expected when consumer is using the subscribe API. Consumes
     * first set of messages and does a commit async, then verifies that consuming a second time does not yield any
     * duplicate message.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommitAsyncWithSubscribe() throws SerializerException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.setManuallyCommitMessageIds(true);
        pscConsumerRunner.setAsyncCommit(true);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        // give enough time for async commit to complete
        Thread.sleep(PSC_CONSUME_TIMEOUT_MS);
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        Iterator<PscConsumerMessage<String, String>> messageIterator = pscConsumerRunner.getMessages().iterator();
        Map<Integer, MessageId> lastMessageIds = new HashMap<>();
        MessageId nextMessageId;
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            lastMessageIds.put(nextMessageId.getTopicUriPartition().getPartition(), nextMessageId);
        }

        // start consuming again
        pscConsumerRunner.setManuallyCommitMessageIds(false);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messageIterator = pscConsumerRunner.getMessages().iterator();

        // verify that none of the earlier messages are re-consumed
        assertTrue(messageIterator.hasNext());
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            assertTrue(
                    !lastMessageIds.containsKey(nextMessageId.getTopicUriPartition().getPartition()) ||
                            nextMessageId.getOffset() > lastMessageIds.get(nextMessageId.getTopicUriPartition().getPartition()).getOffset()
            );
        }
    }

    /**
     * Verifies that PSC consumer async commit works as expected when consumer is using the assign API. Consumes
     * first set of messages and does a commit async, then verifies that consuming a second time does not yield any
     * duplicate message.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommitAsyncWithAssign() throws SerializerException, InterruptedException {
        int messageCount1 = 1000;
        int targetPartition1 = 1;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition1);
        int messageCount2 = 1000;
        int targetPartition2 = 2;
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, targetPartition1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, targetPartition2);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(Sets.newHashSet(topicUriPartition1, topicUriPartition2)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.setManuallyCommitMessageIds(true);
        pscConsumerRunner.setAsyncCommit(true);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        // give enough time for async commit to complete
        Thread.sleep(PSC_CONSUME_TIMEOUT_MS);
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition1);
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition2);

        Iterator<PscConsumerMessage<String, String>> messageIterator = pscConsumerRunner.getMessages().iterator();
        Map<Integer, MessageId> lastMessageIds = new HashMap<>();
        MessageId nextMessageId;
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            lastMessageIds.put(nextMessageId.getTopicUriPartition().getPartition(), nextMessageId);
        }

        // start consuming again
        pscConsumerRunner.setManuallyCommitMessageIds(false);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messageIterator = pscConsumerRunner.getMessages().iterator();

        // verify that none of the earlier messages are re-consumed
        assertTrue(messageIterator.hasNext());
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            assertTrue(nextMessageId.getTopicUriPartition().getPartition() == targetPartition1 || nextMessageId.getTopicUriPartition().getPartition() == targetPartition2);
            assertTrue(
                    !lastMessageIds.containsKey(nextMessageId.getTopicUriPartition().getPartition()) ||
                            nextMessageId.getOffset() > lastMessageIds.get(nextMessageId.getTopicUriPartition().getPartition()).getOffset()
            );
        }
    }

    /**
     * Verifies that PSC consumer async commit of given message ids works as expected when consumer is using the
     * subscribe API. Consumes first set of messages and does a commit async on a few message ids, then verifies that
     * consuming a second time does not yield any duplicate message from topic URI partitions with committed offsets.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommitAsyncOfMessageIdsWithSubscribe() throws SerializerException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.setManuallyCommitMessageIds(true);
        pscConsumerRunner.setAsyncCommit(true);
        pscConsumerRunner.setCommitSelectMessageIds(true);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        // give enough time for async commit to complete
        Thread.sleep(PSC_CONSUME_TIMEOUT_MS);

        Iterator<PscConsumerMessage<String, String>> messageIterator = pscConsumerRunner.getMessages().iterator();
        Map<Integer, MessageId> lastMessageIdByPartition = new HashMap<>();
        MessageId nextMessageId;
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            lastMessageIdByPartition.put(nextMessageId.getTopicUriPartition().getPartition(), nextMessageId);
        }

        Set<MessageId> committedMessageIds = pscConsumerRunner.getCommitted();
        System.out.printf("Committed offset for %d partitions.", committedMessageIds.size());
        assertFalse(committedMessageIds.isEmpty());
        Map<Integer, MessageId> committedMessageIdByPartition = new HashMap<>();
        for (MessageId committedMessageId : committedMessageIds) {
            assertFalse(committedMessageIdByPartition.containsKey(committedMessageId.getTopicUriPartition().getPartition()));
            committedMessageIdByPartition.put(committedMessageId.getTopicUriPartition().getPartition(), committedMessageId);
        }

        // start consuming again
        pscConsumerRunner.setManuallyCommitMessageIds(false);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messageIterator = pscConsumerRunner.getMessages().iterator();

        // verify that none of the earlier messages are re-consumed
        assertTrue(messageIterator.hasNext());
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            if (committedMessageIdByPartition.containsKey(nextMessageId.getTopicUriPartition().getPartition())) {
                assertTrue(nextMessageId.getOffset() >
                        committedMessageIdByPartition.get(nextMessageId.getTopicUriPartition().getPartition()).getOffset());
            } else {
                assertTrue(nextMessageId.getOffset() <=
                        lastMessageIdByPartition.get(nextMessageId.getTopicUriPartition().getPartition()).getOffset());
            }
        }
    }

    /**
     * Verifies that PSC consumer async commit of given message ids works as expected when consumer is using the
     * assign API. Consumes first set of messages and does a commit async on a few message ids, then verifies that
     * consuming a second time does not yield any duplicate message from topic URI partitions with committed offsets.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommitAsyncOfMessageIdsWithAssign() throws SerializerException, InterruptedException {
        int messageCount1 = 1000;
        int targetPartition1 = 1;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition1);
        int messageCount2 = 1000;
        int targetPartition2 = 2;
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, targetPartition1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, targetPartition2);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(Sets.newHashSet(topicUriPartition1, topicUriPartition2)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.setManuallyCommitMessageIds(true);
        pscConsumerRunner.setAsyncCommit(true);
        pscConsumerRunner.setCommitSelectMessageIds(true);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        // give enough time for async commit to complete
        Thread.sleep(PSC_CONSUME_TIMEOUT_MS);
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition1);
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition2);

        Iterator<PscConsumerMessage<String, String>> messageIterator = pscConsumerRunner.getMessages().iterator();
        Map<Integer, MessageId> lastMessageIdByPartition = new HashMap<>();
        MessageId nextMessageId;
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            lastMessageIdByPartition.put(nextMessageId.getTopicUriPartition().getPartition(), nextMessageId);
        }

        Set<MessageId> committedMessageIds = pscConsumerRunner.getCommitted();
        System.out.printf("Committed offset for %d partitions.", committedMessageIds.size());
        assertFalse(committedMessageIds.isEmpty());
        Map<Integer, MessageId> committedMessageIdByPartition = new HashMap<>();
        for (MessageId committedMessageId : committedMessageIds) {
            assertFalse(committedMessageIdByPartition.containsKey(committedMessageId.getTopicUriPartition().getPartition()));
            committedMessageIdByPartition.put(committedMessageId.getTopicUriPartition().getPartition(), committedMessageId);
        }

        // start consuming again
        pscConsumerRunner.setManuallyCommitMessageIds(false);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messageIterator = pscConsumerRunner.getMessages().iterator();

        // verify that none of the earlier messages are re-consumed
        assertTrue(messageIterator.hasNext());
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            if (committedMessageIdByPartition.containsKey(nextMessageId.getTopicUriPartition().getPartition())) {
                assertTrue(nextMessageId.getOffset() >
                        committedMessageIdByPartition.get(nextMessageId.getTopicUriPartition().getPartition()).getOffset());
            } else {
                assertTrue(nextMessageId.getOffset() <=
                        lastMessageIdByPartition.get(nextMessageId.getTopicUriPartition().getPartition()).getOffset());
            }
        }
    }

    /**
     * Verifies that PSC consumer commit works as expected when consumer is using the subscribe API.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommitWithSubscribe() throws SerializerException, InterruptedException {
        testCommitWithSubscribe(topicUriStr1);
    }

    /**
     * Verifies that PSC consumer commit works as expected when consumer is using the subscribe API and the topic URI
     * is a topic RN (misses the prefix <code>protocol:/</code>, using the default protocol).
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommitWithSubscribeWithTopicRn() throws SerializerException, InterruptedException {
        testCommitWithSubscribe(topicRnStr1);
    }

    public void testCommitWithSubscribe(String topicUriStr) throws SerializerException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.setManuallyCommitMessageIds(true);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        Iterator<PscConsumerMessage<String, String>> messageIterator = pscConsumerRunner.getMessages().iterator();
        Map<Integer, MessageId> firstMessageIds = new HashMap<>();
        MessageId nextMessageId;
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            if (!firstMessageIds.containsKey(nextMessageId.getTopicUriPartition().getPartition()))
                firstMessageIds.put(nextMessageId.getTopicUriPartition().getPartition(), nextMessageId);
        }
        Map<Integer, MessageId> committed = pscConsumerRunner.getCommitted().stream().collect(Collectors.toMap(
                messageId -> messageId.getTopicUriPartition().getPartition(), Function.identity()
        ));

        // start consuming again
        pscConsumerRunner.setManuallyCommitMessageIds(false);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messageIterator = pscConsumerRunner.getMessages().iterator();

        // verify that the consumption started after the earlier committed messages
        while (messageIterator.hasNext() && !firstMessageIds.isEmpty()) {
            nextMessageId = messageIterator.next().getMessageId();
            int partition = nextMessageId.getTopicUriPartition().getPartition();
            if (committed.containsKey(partition)) {
                assertEquals(committed.get(partition).getOffset(), nextMessageId.getOffset() - 1);
                committed.remove(partition);
                firstMessageIds.remove(partition);
            } else if (firstMessageIds.containsKey(partition)) {
                assertEquals(firstMessageIds.get(partition).getOffset(), nextMessageId.getOffset());
                firstMessageIds.remove(partition);
            }
        }
    }

    /**
     * Verifies that PSC consumer commit works as expected when consumer is using the assign API.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommitWithAssign() throws SerializerException, InterruptedException {
        int messageCount1 = 1000;
        int targetPartition1 = 1;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition1);
        int messageCount2 = 1000;
        int targetPartition2 = 2;
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, targetPartition1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, targetPartition2);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(Sets.newHashSet(topicUriPartition1, topicUriPartition2)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.setManuallyCommitMessageIds(true);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        Iterator<PscConsumerMessage<String, String>> messageIterator = pscConsumerRunner.getMessages().iterator();
        Map<Integer, MessageId> firstMessageIds = new HashMap<>();
        MessageId nextMessageId;
        while (messageIterator.hasNext()) {
            nextMessageId = messageIterator.next().getMessageId();
            if (!firstMessageIds.containsKey(nextMessageId.getTopicUriPartition().getPartition()))
                firstMessageIds.put(nextMessageId.getTopicUriPartition().getPartition(), nextMessageId);
        }
        Map<Integer, MessageId> committed = pscConsumerRunner.getCommitted().stream().collect(Collectors.toMap(
                messageId -> messageId.getTopicUriPartition().getPartition(), Function.identity()
        ));

        // start consuming again
        pscConsumerRunner.setManuallyCommitMessageIds(false);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messageIterator = pscConsumerRunner.getMessages().iterator();

        // verify that the consumption started after the earlier committed messages
        while (messageIterator.hasNext() && !firstMessageIds.isEmpty()) {
            nextMessageId = messageIterator.next().getMessageId();
            int partition = nextMessageId.getTopicUriPartition().getPartition();
            if (committed.containsKey(partition)) {
                assertEquals(committed.get(partition).getOffset(), nextMessageId.getOffset() - 1);
                committed.remove(partition);
                firstMessageIds.remove(partition);
            } else if (firstMessageIds.containsKey(partition)) {
                assertEquals(firstMessageIds.get(partition).getOffset(), nextMessageId.getOffset());
                firstMessageIds.remove(partition);
            }
        }
    }

    /**
     * Verifies that consumer wakeup works as expected. It launches a PSC consumer that relies on one backend consumers,
     * that gets stuck into a poll() call due to large timeout when the underlying topic has no new messages. Then it
     * verifies that calling wakeup() on PSC consumer actually interrupts the backend consumer and as a result
     * interrupts the PSC consumer.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testWakeupWithLongRunningPoll() throws SerializerException, InterruptedException {
        PscTestUtils.produceKafkaMessages(1000, sharedKafkaTestResource, kafkaCluster, topic1);
        PscTestUtils.produceKafkaMessages(0, sharedKafkaTestResource, kafkaCluster, topic2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_TIMEOUT_MS, Integer.MAX_VALUE);

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Sets.newHashSet(topicUriStr1, topicUriStr2)), PSC_CONSUME_TIMEOUT_MS
        );

        // since one topic has no messages ans poll timeout is set to max, the poll will be stuck
        pscConsumerRunner.kickoff();
        // wait for PSC consumer to be initialized
        Thread.sleep(1000);
        // wake up the consumer to interrupt the stuck poll call
        pscConsumerRunner.wakeupConsumer();
        // wait for wake up call to process
        Thread.sleep(1000);
        assertEquals(WakeupException.class, pscConsumerRunner.getException().getClass());
    }

    /**
     * Verifies that consumer wakeup works as expected. It first produces messages to a topic and creates a PscConsumer
     * to consume them all. Then it launches another PscConsumer and immediately calls <code>wakeup()</code> on it.
     * Then, makes a <code>getPartitions</code> call on the topic messages were produced to, and verifies that the call
     * does not complete and instead a <code>WakeupException</code> is thrown.
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws ConsumerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testWakeupWithGetPartitions() throws SerializerException, InterruptedException, ConsumerException, ConfigurationException {
        PscTestUtils.produceKafkaMessages(1000, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Sets.newHashSet(topicUriStr1, topicUriStr2)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();

        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.wakeup();
        assertThrows(WakeupException.class, () -> pscConsumer.getPartitions(topicUriStr1));
        pscConsumer.close();
    }

    /**
     * Verifies that seek to beginning works as expected when PSC consumer uses the subscribe() API. The consumer first
     * consumes all messages in the topic, then calls seekToBeginning on a couple of partitions. When it resumes
     * consumption of the same topic (with the same old messages) it consumes only messages from those couple of
     * partitions.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekToBeginningWithSubscribe() throws SerializerException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(0, messages.size());

        // seek to beginning of the first two partitions and verify that the first message read from each seeked
        // partition is the one we expect
        int partition0 = 0;
        int partition1 = 1;

        pscConsumerRunner.setPartitionTimestampsToSeek(
                new HashMap<TopicUriPartition, Long>() {{
                    put(new TopicUriPartition(topicUriStr1, partition0), Long.MIN_VALUE);
                    put(new TopicUriPartition(topicUriStr1, partition1), Long.MIN_VALUE);
                }}
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        Iterator<PscConsumerMessage<String, String>> messageIterator = pscConsumerRunner.getMessages().iterator();
        assertTrue(messageIterator.hasNext());
        long offset0 = 0;
        long offset1 = 0;
        MessageId messageId;
        while (messageIterator.hasNext()) {
            messageId = messageIterator.next().getMessageId();
            if (messageId.getTopicUriPartition().getPartition() == partition0) {
                assertEquals(offset0, messageId.getOffset());
                ++offset0;
            } else if (messageId.getTopicUriPartition().getPartition() == partition1) {
                assertEquals(offset1, messageId.getOffset());
                ++offset1;
            } else
                fail("Consumed a message from a partition that was not seeked to beginning: " + messageId);
        }
    }

    /**
     * Verifies that seek to end works as expected when PSC consumer uses the subscribe() API. The consumer first
     * seeks to end of a couple of partitions and then starts consuming. The test verifies that it does not consume
     * any message from those couple of partitions.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSeekToEndWithSubscribe() throws SerializerException, InterruptedException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );

        // seek to end of the first two partitions and verify that the no messages are read from any of them
        int partition0 = 0;
        int partition1 = 1;

        pscConsumerRunner.setPartitionTimestampsToSeek(
                new HashMap<TopicUriPartition, Long>() {{
                    put(new TopicUriPartition(topicUriStr1, partition0), Long.MAX_VALUE);
                    put(new TopicUriPartition(topicUriStr1, partition1), Long.MAX_VALUE);
                }}
        );

        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertTrue(messages.size() < messageCount);
        for (PscConsumerMessage<String, String> message : messages) {
            assertNotEquals(partition0, message.getMessageId().getTopicUriPartition().getPartition());
            assertNotEquals(partition1, message.getMessageId().getTopicUriPartition().getPartition());
        }
    }

    /**
     * Verifies that PscConsumer that is using the subscribe API and auto-commit returns correct committed offset for
     * topic URI partitions. It first generates a specific number of message to a topic. Then a PscConsumer subscribes
     * and all the messages. Then calls on committed() for each partition of the topic and verifies that the returned
     * offset matches that of the last message consumed from that partition.
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws WakeupException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommittedWithSubscribeAndAutoCommit() throws SerializerException, InterruptedException, ConsumerException, ConfigurationException, WakeupException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();

        // find last message from each partition
        Map<Integer, MessageId> lastMessageIdByPartition = new HashMap<>();
        for (PscConsumerMessage<String, String> message : messages)
            lastMessageIdByPartition.put(message.getMessageId().getTopicUriPartition().getPartition(), message.getMessageId());

        // verify the last message id from each partition is the one returned from committed
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        for (int i = 0; i < partitions1; ++i) {
            assertEquals(
                    lastMessageIdByPartition.get(i).getOffset() + 1,
                    pscConsumer.committed(new TopicUriPartition(topicUriStr1, i)).getOffset()
            );
        }
        pscConsumer.close();
    }

    /**
     * Verifies that PscConsumer that is using the subscribe API and manual commit returns correct committed offset for
     * topic URI partitions. It first generates a specific number of message to a topic. Then a PscConsumer subscribes
     * to the topic and consumes all the messages, but commits offsets for only a few of them. Then calls on committed()
     * for each partition of the topic and verifies that the returned offset matches that of the last message consumed
     * from that partition (or null if no offset was committed).
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws WakeupException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommittedWithSubscribeAndManualCommit() throws SerializerException, InterruptedException, ConsumerException, ConfigurationException, WakeupException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.setManuallyCommitMessageIds(true);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        Map<Integer, MessageId> committed = pscConsumerRunner.getCommitted().stream().collect(Collectors.toMap(
                messageId -> messageId.getTopicUriPartition().getPartition(), Function.identity()
        ));

        // verify that committed() returns null for partitions with no committed offset and the correct offset from the
        // partitions with a committed offset
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        for (int i = 0; i < partitions1; ++i) {
            if (committed.get(i) != null)
                assertEquals(committed.get(i).getOffset() + 1, pscConsumer.committed(new TopicUriPartition(topicUriStr1, i)).getOffset());
            else
                assertNull(pscConsumer.committed(new TopicUriPartition(topicUriStr1, i)));
        }
        pscConsumer.close();
    }

    /**
     * Verifies that PscConsumer that is using the assign API and auto-commit returns correct committed offset for
     * topic URI partitions. It first generates a specific number of message to a couple of partitions. Then consumes
     * all those messages. Then calls on committed() for each partition of the topic and verifies that the returned
     * offset matches that of the last committed message consumed from that partition (if the partition was assigned)or
     * null (if it was not).
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws WakeupException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommittedWithAssignAndAutoCommit() throws SerializerException, InterruptedException, ConsumerException, ConfigurationException, WakeupException {
        int messageCount1 = 1000;
        int targetPartition1 = 1;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition1);
        int messageCount2 = 1000;
        int targetPartition2 = 2;
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, targetPartition1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, targetPartition2);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(Sets.newHashSet(topicUriPartition1, topicUriPartition2)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();

        // find last message from each partition
        Map<Integer, MessageId> lastMessageIdByPartition = new HashMap<>();
        for (PscConsumerMessage<String, String> message : messages)
            lastMessageIdByPartition.put(message.getMessageId().getTopicUriPartition().getPartition(), message.getMessageId());

        // verify the last message id from each partition is the one returned from committed
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        for (int i = 0; i < partitions1; ++i) {
            if (i == targetPartition1 || i == targetPartition2) {
                assertEquals(
                        lastMessageIdByPartition.get(i).getOffset() + 1,
                        pscConsumer.committed(new TopicUriPartition(topicUriStr1, i)).getOffset()
                );
            } else
                assertNull(pscConsumer.committed(new TopicUriPartition(topicUriStr1, i)));
        }
        pscConsumer.close();
    }

    /**
     * Verifies that PscConsumer that is using the assign API and manual commit returns correct committed offset for
     * topic URI partitions. It first generates a specific number of message to a couple of partitions. Then consumes
     * from them and commits offsets for them. Then calls on committed() for each partition of the topic and verifies
     * that the returned offset matches that of the last committed message consumed from that partition (or null if none
     * was committed)
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws WakeupException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCommittedWithAssignAndManualCommit() throws SerializerException, InterruptedException, ConsumerException, ConfigurationException, WakeupException {
        int messageCount1 = 1000;
        int targetPartition1 = 1;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition1);
        int messageCount2 = 1000;
        int targetPartition2 = 2;
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");

        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, targetPartition1);
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, targetPartition2);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(Sets.newHashSet(topicUriPartition1, topicUriPartition2)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.setManuallyCommitMessageIds(true);
        pscConsumerRunner.kickoffAndWaitForCompletion();
        Map<Integer, MessageId> committed = pscConsumerRunner.getCommitted().stream().collect(Collectors.toMap(
                messageId -> messageId.getTopicUriPartition().getPartition(), Function.identity()
        ));

        // verify that committed() returns null for partitions with no committed offset and the correct offset from the
        // partitions with a committed offset
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        for (int i = 0; i < partitions1; ++i) {
            if (committed.get(i) != null)
                assertEquals(committed.get(i).getOffset() + 1, pscConsumer.committed(new TopicUriPartition(topicUriStr1, i)).getOffset());
            else
                assertNull(pscConsumer.committed(new TopicUriPartition(topicUriStr1, i)));
        }
        pscConsumer.close();
    }

    /**
     * Verifies that the startOffsets() api returns the expected result, which is 0 for partitions with no messages, and
     * first message's offset for partitions that have messages.
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testStartOffsets() throws ConsumerException, ConfigurationException, SerializerException, InterruptedException {
        // first check the case where topic has no messages yet
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        Set<TopicUriPartition> topicUriPartitions = new HashSet<>();
        for (int i = 0; i < partitions1; ++i)
            topicUriPartitions.add(new TopicUriPartition(topicUriStr1, i));
        Map<TopicUriPartition, Long> startOffsets = pscConsumer.startOffsets(topicUriPartitions);
        assertEquals(partitions1, startOffsets.size());
        for (Map.Entry<TopicUriPartition, Long> entry : startOffsets.entrySet())
            assertEquals(new Long(0L), entry.getValue());

        // next check the case where topic has messages
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        // use another consumer to consume all messages
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        // find first message from each partition
        Map<Integer, MessageId> firstMessageIdByPartition = new HashMap<>();
        for (PscConsumerMessage<String, String> message : messages)
            firstMessageIdByPartition.putIfAbsent(message.getMessageId().getTopicUriPartition().getPartition(), message.getMessageId());

        startOffsets = pscConsumer.startOffsets(topicUriPartitions);
        assertEquals(partitions1, startOffsets.size());
        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            int partition = topicUriPartition.getPartition();
            assertEquals(topicUriPartition.getTopicUriAsString(), firstMessageIdByPartition.get(partition).getTopicUriPartition().getTopicUriAsString());
            assertEquals(firstMessageIdByPartition.get(partition).getOffset(), startOffsets.get(topicUriPartition).longValue());
        }

        pscConsumer.close();
    }

    /**
     * Verifies that the endOffsets() api returns the expected result, which is 0 for partitions with no messages, and
     * last message's offset + 1 for partitions that have messages.
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testEndOffsets() throws ConsumerException, ConfigurationException, SerializerException, InterruptedException {
        // first check the case where topic has no messages yet
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        Set<TopicUriPartition> topicUriPartitions = new HashSet<>();
        for (int i = 0; i < partitions1; ++i)
            topicUriPartitions.add(new TopicUriPartition(topicUriStr1, i));
        Map<TopicUriPartition, Long> endOffsets = pscConsumer.endOffsets(topicUriPartitions);
        assertEquals(partitions1, endOffsets.size());
        for (Map.Entry<TopicUriPartition, Long> entry : endOffsets.entrySet())
            assertEquals(new Long(0L), entry.getValue());

        // next check the case where topic has messages
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        // use another consumer to consume all messages
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        // find last message from each partition
        Map<Integer, MessageId> lastMessageIdByPartition = new HashMap<>();
        for (PscConsumerMessage<String, String> message : messages)
            lastMessageIdByPartition.put(message.getMessageId().getTopicUriPartition().getPartition(), message.getMessageId());

        endOffsets = pscConsumer.endOffsets(topicUriPartitions);
        assertEquals(partitions1, endOffsets.size());
        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            int partition = topicUriPartition.getPartition();
            assertEquals(topicUriPartition.getTopicUriAsString(), lastMessageIdByPartition.get(partition).getTopicUriPartition().getTopicUriAsString());
            assertEquals(lastMessageIdByPartition.get(partition).getOffset() + 1, endOffsets.get(topicUriPartition).longValue());
        }

        pscConsumer.close();
    }

    private void testPositionWithSubscribe(boolean commitOffsets) throws ConsumerException, ConfigurationException, SerializerException {
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, 1);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_TIMEOUT_MS, 10);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, Boolean.toString(commitOffsets));
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);

        // first check the case where consumer is not subscribed to or assigned the partition
        Set<TopicUriPartition> topicUriPartitions = new HashSet<>();
        for (int i = 0; i < partitions1; ++i)
            topicUriPartitions.add(new TopicUriPartition(topicUriStr1, i));
        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            Exception e = assertThrows(ConsumerException.class, () -> pscConsumer.position(topicUriPartition));
            assertEquals(ExceptionMessage.NO_SUBSCRIPTION_ASSIGNMENT, e.getMessage());
        }

        // next subscribe to topic and check the case with empty partitions (they should throw exceptions since consumer
        // has no assigned partition yet - as it hasn't called poll() yet).
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        for (TopicUriPartition topicUriPartition : topicUriPartitions)
            assertThrows(ConsumerException.class, () -> pscConsumer.position(topicUriPartition));

        // produce messages to topic and verify position as consumption happens
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);
        int messagesConsumed = 0;
        PscConsumerPollMessageIterator<String, String> messageIterator;
        PscConsumerMessage<String, String> message;
        while (messagesConsumed < messageCount) {
            messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                message = messageIterator.next();
                assertEquals(
                        message.getMessageId().getOffset() + 1,
                        pscConsumer.position(new TopicUriPartition(
                                message.getMessageId().getTopicUriPartition().getTopicUriAsString(),
                                message.getMessageId().getTopicUriPartition().getPartition()
                        ))
                );
                ++messagesConsumed;
            }
        }

        pscConsumer.close();
    }

    /**
     * Verifies that consumer position() api returns the expected offset when consumer is using the subscribe API and
     * committing offsets.
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws SerializerException
     * @throws WakeupException
     */
    @Timeout(2 * TEST_TIMEOUT_SECONDS)
    @Test
    public void testPositionWithSubscribeAndOffsetCommit() throws ConsumerException, ConfigurationException, SerializerException, WakeupException {
        testPositionWithSubscribe(true);
    }

    /**
     * Verifies that consumer position() api returns the expected offset when consumer is using the subscribe API and
     * not committing offsets.
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws SerializerException
     * @throws WakeupException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testPositionWithSubscribeAndNoOffsetCommit() throws ConsumerException, ConfigurationException, SerializerException, WakeupException {
        testPositionWithSubscribe(false);
    }

    private void testPositionWithAssign(boolean commitOffsets) throws ConsumerException, ConfigurationException, SerializerException, TopicUriSyntaxException {
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, 1);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_TIMEOUT_MS, 10);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, Boolean.toString(commitOffsets));
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);

        // first check the case where consumer is not subscribed to or assigned the partition
        // skip: already tested in testPositionWithSubscribe

        // next assign partitions to consumer and check the case with empty partitions (they should throw exceptions since consumer
        // has no assigned partition yet - as it hasn't called poll() yet).
        int messageCount1 = 500;
        int targetPartition1 = 1;
        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, targetPartition1);
        int messageCount2 = 500;
        int targetPartition2 = 2;
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, targetPartition2);

        pscConsumer.assign(Sets.newHashSet(topicUriPartition1, topicUriPartition2));

        // verify that a wakeup() interrupts the position() call.
        pscConsumer.wakeup();
        assertThrows(WakeupException.class, () -> pscConsumer.position(topicUriPartition1));

        for (int i = 0; i < partitions1; ++i) {
            TopicUriPartition topicUriPartition = TestUtils.getFinalizedTopicUriPartition(
                    KafkaTopicUri.validate(TopicUri.validate(topicUriStr1)),
                    i
            );
            if (topicUriPartition.equals(topicUriPartition1) || topicUriPartition.equals(topicUriPartition2))
                assertEquals(0, pscConsumer.position(topicUriPartition));
            else
                assertThrows(ConsumerException.class, () -> pscConsumer.position(topicUriPartition));
        }

        // produce messages to partitions and verify position as consumption happens
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition1);
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource, kafkaCluster, topic1, targetPartition2);

        int messagesConsumed = 0;
        PscConsumerPollMessageIterator<String, String> messageIterator;
        PscConsumerMessage<String, String> message;
        while (messagesConsumed < messageCount1 + messageCount2) {
            messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                message = messageIterator.next();
                assertEquals(
                        message.getMessageId().getOffset() + 1,
                        pscConsumer.position(new TopicUriPartition(
                                message.getMessageId().getTopicUriPartition().getTopicUriAsString(),
                                message.getMessageId().getTopicUriPartition().getPartition()
                        ))
                );
                ++messagesConsumed;
            }
        }

        pscConsumer.close();
    }

    /**
     * Verifies that consumer position() api returns the expected offset when consumer is using the assign API and
     * committing offsets.
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws SerializerException
     * @throws WakeupException
     * @throws TopicUriSyntaxException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testPositionWithAssignAndOffsetCommit() throws ConsumerException, ConfigurationException, SerializerException, WakeupException, TopicUriSyntaxException {
        testPositionWithAssign(true);
    }

    /**
     * Verifies that consumer position() api returns the expected offset when consumer is using the assign API and not
     * committing offsets.
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     * @throws SerializerException
     * @throws WakeupException
     * @throws TopicUriSyntaxException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testPositionWithAssignAndNoOffsetCommit() throws ConsumerException, ConfigurationException, SerializerException, WakeupException, TopicUriSyntaxException {
        testPositionWithAssign(false);
    }

    /**
     * Verifies that seeking to timestamps works as expected when PSC consumer is using the subscribe() API. This test
     * tries seeking to a specific timestamp for a couple of partitions and verifies that the pointer on those
     * partitions after seek is pointing to the correct message.
     *
     * @throws SerializerException
     * @throws InterruptedException
     * @throws ConsumerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testMessageIdByTimestamp() throws SerializerException, InterruptedException, ConsumerException, ConfigurationException {
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);

        Set<TopicUriPartition> topicUriPartitions = new HashSet<>();
        for (int i = 0; i < partitions1; ++i)
            topicUriPartitions.add(new TopicUriPartition(topicUriStr1, i));

        long timestamp1 = System.currentTimeMillis();
        // the case where there is no messages in topic yet
        Map<TopicUriPartition, MessageId> messageIdByTimestamp =
                pscConsumer.getMessageIdByTimestamp(topicUriPartitions.stream().collect(Collectors.toMap(
                        topicUriPartition -> topicUriPartition,
                        topicUriPartition -> timestamp1
                )));
        messageIdByTimestamp.forEach((key, value) -> assertNull(value));

        // the case where the first batch of messages is published to topic
        int messageCount = 10000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        Thread.sleep(5000);
        messageIdByTimestamp = pscConsumer.getMessageIdByTimestamp(topicUriPartitions.stream().collect(Collectors.toMap(
                topicUriPartition -> topicUriPartition,
                topicUriPartition -> timestamp1
        )));
        messageIdByTimestamp.forEach((key, value) -> assertEquals(0, value.getOffset()));
        Map<TopicUriPartition, Long> endOffsets = pscConsumer.endOffsets(topicUriPartitions);

        // the case where the second batch of messages is published to topic
        long timestamp2 = System.currentTimeMillis();
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);
        Thread.sleep(5000);
        messageIdByTimestamp = pscConsumer.getMessageIdByTimestamp(topicUriPartitions.stream().collect(Collectors.toMap(
                topicUriPartition -> topicUriPartition,
                topicUriPartition -> timestamp2
        )));
        messageIdByTimestamp.forEach((key, value) -> assertEquals(
                endOffsets.get(key).longValue(),
                value.getOffset())
        );

        pscConsumer.close();
    }

    /**
     * Verifies that in case of a non-existing topic URI the <code>getPartition()</code> call returns proper result.
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testGetPartitionsOfNonExistingTopicUri() throws ConsumerException, ConfigurationException {
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(pscConfiguration);
        String topicUriStr = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), "not_there");
        assertNull(pscConsumer.getPartitions(topicUriStr));
        pscConsumer.close();
    }

    /**
     * Verifies that PscConsumer.listTopicRNs returns the correct list of topic RNs given a regex pattern
     *
     * @throws ConsumerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testListTopics() throws ConsumerException, ConfigurationException {
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER,
            StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER,
            StringDeserializer.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);

        Set<String> topicsInCluster = new HashSet<>();
        String
            topicRn =
            String.format("rn:kafka:env:cloud_%s::%s", kafkaCluster.getRegion(),
                kafkaCluster.getCluster());
        topicsInCluster.add(topicRn + ":" + topic1);
        topicsInCluster.add(topicRn + ":" + topic2);
        topicsInCluster.add(topicRn + ":" + topic3);
        Pattern topicPattern = Pattern.compile("topic[0-9]+");
        pscConsumer.listTopicRNs(topicRn, topicPattern).forEach((topic) -> {
            assertTrue(topicsInCluster.contains(topic));
        });
        topicPattern = Pattern.compile("topic");
        assertTrue(pscConsumer.listTopicRNs(topicRn, topicPattern).isEmpty());
        pscConsumer.close();
    }

    /**
     * Verifies that the reset backend consumer feature of <code>PscConsumer</code> works as expected when consumer is
     * using the <code>subscribe</code> and <code>poll</code> APIs.
     *
     * @throws SerializerException
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testResetBackendConsumerWithSubscription() throws SerializerException, ConfigurationException, ConsumerException {
        int messageCount = 100;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);

        // do an initial consumption
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        PscConsumerPollMessageIterator<String, String> iterator = pscConsumer.poll();
        while (!iterator.hasNext())
            iterator = pscConsumer.poll();

        assertEquals(1, PscConsumerUtils.getBackendConsumersOf(pscConsumer).size());
        PscBackendConsumer<String, String> pscBackendConsumer1 = PscConsumerUtils.getBackendConsumersOf(pscConsumer).iterator().next();
        Map<Integer, Long> lastOffsets = new HashMap<>();
        while (iterator.hasNext()) {
            MessageId messageId = iterator.next().getMessageId();
            lastOffsets.put(messageId.getTopicUriPartition().getPartition(), messageId.getOffset());
        }
        pscConsumer.commitSync();

        // produce more messages just in case all messages were consumed last time
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        // reset backend consumer and consume again
        PscConsumerUtils.resetBackendConsumer(pscConsumer, pscBackendConsumer1);
        iterator = pscConsumer.poll();
        while (!iterator.hasNext())
            iterator = pscConsumer.poll();

        assertEquals(1, PscConsumerUtils.getBackendConsumersOf(pscConsumer).size());
        PscBackendConsumer<String, String> pscBackendConsumer2 = PscConsumerUtils.getBackendConsumersOf(pscConsumer).iterator().next();
        assertNotEquals(pscBackendConsumer1, pscBackendConsumer2);
        while (iterator.hasNext()) {
            MessageId messageId = iterator.next().getMessageId();
            int partition = messageId.getTopicUriPartition().getPartition();
            long offset = messageId.getOffset();
            if (lastOffsets.containsKey(partition)) {
                assertEquals(lastOffsets.get(partition) + 1, offset);
                lastOffsets.remove(partition);
            }
        }

        pscConsumer.close();
    }

    /**
     * Verifies that the reset backend consumer feature of <code>PscConsumer</code> works as expected when consumer is
     * using the <code>assign</code> and <code>poll</code> APIs.
     *
     * @throws SerializerException
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testResetBackendConsumerWithAssignment() throws SerializerException, ConfigurationException, ConsumerException {
        int messageCount = 100;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);

        // do an initial consumption
        pscConsumer.assign(Collections.singleton(new TopicUriPartition(topicUriStr1, 0)));
        PscConsumerPollMessageIterator<String, String> iterator = pscConsumer.poll();
        while (!iterator.hasNext())
            iterator = pscConsumer.poll();

        assertTrue(iterator.hasNext());
        assertEquals(1, PscConsumerUtils.getBackendConsumersOf(pscConsumer).size());
        PscBackendConsumer<String, String> pscBackendConsumer1 = PscConsumerUtils.getBackendConsumersOf(pscConsumer).iterator().next();
        PscConsumerMessage<String, String> next = null;
        while (iterator.hasNext()) {
            next = iterator.next();
        }
        long lastOffset = next.getMessageId().getOffset();
        pscConsumer.commitSync();

        // produce more messages just in case all messages were consumed last time
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        // reset backend consumer and consume again
        PscConsumerUtils.resetBackendConsumer(pscConsumer, pscBackendConsumer1);
        iterator = pscConsumer.poll();
        while (!iterator.hasNext())
            iterator = pscConsumer.poll();

        assertEquals(1, PscConsumerUtils.getBackendConsumersOf(pscConsumer).size());
        PscBackendConsumer<String, String> pscBackendConsumer2 = PscConsumerUtils.getBackendConsumersOf(pscConsumer).iterator().next();
        assertNotEquals(pscBackendConsumer1, pscBackendConsumer2);
        next = iterator.next();
        assertEquals(lastOffset + 1, next.getMessageId().getOffset());

        pscConsumer.close();
    }

    /**
     * Verifies that consumer interceptors (<code>onConsume</code> and <code>onCommit</code>) are properly called after
     * message consumption and offset commit, either when interceptor objects are used or when interceptor class names
     * are used.
     *
     * @throws SerializerException
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testInterceptorsWithCommitSync() throws SerializerException, ConfigurationException, ConsumerException {
        int messageCount = 1000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        IdentityInterceptor<String, String> identityInterceptor = new IdentityInterceptor<>();

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_INTERCEPTORS_TYPED_CLASSES, identityInterceptor);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES, IdentityInterceptor.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        assertEquals(1, pscConsumer.getRawInterceptors().size());
        IdentityInterceptor<byte[], byte[]> rawInterceptor = (IdentityInterceptor<byte[], byte[]>) pscConsumer.getRawInterceptors().get(0);

        // do an initial consumption
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        PscConsumerPollMessageIterator<String, String> iterator = pscConsumer.poll();
        while (!iterator.hasNext())
            iterator = pscConsumer.poll();

        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            ++count;
        }
        assertEquals(count, identityInterceptor.onConsumeCounter);
        assertEquals(count, rawInterceptor.onConsumeCounter);

        pscConsumer.commitSync();
        assertEquals(partitions1, identityInterceptor.onCommitCounter);
        assertEquals(partitions1, rawInterceptor.onCommitCounter);

        pscConsumer.close();
    }

    /**
     * Verifies that backend consumer creation/extraction works as expected; i.e. the correct backend consumer is
     * returned given the PSC consumer configuration.
     *
     * @throws TopicUriSyntaxException
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConsumerCreator() throws TopicUriSyntaxException, ConfigurationException, ConsumerException {
        PscKafkaConsumerCreator<String, String> creator = new PscKafkaConsumerCreator<>();
        TopicUri topicUri = TopicUri.validate("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:");
        TopicUri kafkaTopicUri = KafkaTopicUri.validate(topicUri);

        PscConfigurationInternal pscConfigurationInternal = new PscConfigurationInternal(pscConfiguration, PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);

        Set<PscBackendConsumer<String, String>> consumers = creator.getConsumers(
                pscConfigurationInternal.getEnvironment(),
                pscConfigurationInternal,
                null,
                Collections.singleton(kafkaTopicUri),
                false,
                false
        );

        consumers.forEach(consumer -> {
            PscConfiguration configuration = consumer.getConfiguration();
            Assertions.assertEquals(kafkaCluster.getBootstrap(),
                    configuration.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
            Assertions.assertEquals(ByteArrayDeserializer.class.getName(),
                    configuration.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
            Assertions.assertEquals(ByteArrayDeserializer.class.getName(),
                    configuration.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
            Assertions.assertEquals("PLAINTEXT",
                    configuration.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
            try {
                consumer.close();
            } catch (Exception exception) {
                // OK
            }
        });
    }

    /**
     * Verifies that the poll result can be properly broken down into sub-iterators by topic uri partition.
     *
     * @throws SerializerException
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testIteratorFor() throws SerializerException, ConfigurationException, ConsumerException {
        int messageCount = 10000;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Arrays.asList(topicUriStr1));

        final AtomicInteger countSub = new AtomicInteger(0);
        final AtomicInteger countAll = new AtomicInteger(0);
        PscConsumerPollMessageIterator<String, String> result = null;
        while (countSub.get() < messageCount || countAll.get() < messageCount) {
            result = pscConsumer.poll();

            for (TopicUriPartition topicUriPartition : result.getTopicUriPartitions()) {
                PscConsumerPollMessageIterator<String, String> iterator = result.iteratorFor(topicUriPartition);
                iterator.forEachRemaining(message -> countSub.incrementAndGet());
                assertFalse(iterator.hasNext());
            }

            result.forEachRemaining(message -> countAll.incrementAndGet());
        }

        assertEquals(messageCount, countSub.get());
        assertEquals(messageCount, countAll.get());

        assertEquals(partitions1, pscConsumer.assignment().size());

        pscConsumer.close();
    }

    /**
     * Verifies that offset commits and seek work continuously and seamlessly together across different
     * consumptions (Kafka, PSC).
     *
     * @throws SerializerException
     * @throws ConfigurationException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConsumptionContinuity() throws SerializerException, ConfigurationException, ConsumerException {
        int messageCount = 500;
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic3);

        ConsumerRecord lastKafkaMessage = PscTestUtils.consumeKafkaMessages(100, sharedKafkaTestResource, kafkaCluster, topic3, 0, -1);
        // Kafka consumer committed offset is the next offset to fetch
        long lastConsumedOffset = lastKafkaMessage.offset();
        assertEquals("99", PscCommon.byteArrayToString((byte[]) lastKafkaMessage.value()));
        assertEquals(99, lastConsumedOffset);

        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUriStr3, 0);
        PscConsumerMessage lastPscMessage = PscTestUtils.consumePscMessages(100, topicUriPartition, lastConsumedOffset);
        lastConsumedOffset = lastPscMessage.getMessageId().getOffset();
        assertEquals("199", PscCommon.byteArrayToString((byte[]) lastPscMessage.getValue()));
        assertEquals(199, lastConsumedOffset);

        lastPscMessage = PscTestUtils.consumePscMessages(100, topicUriPartition, lastConsumedOffset);
        lastConsumedOffset = lastPscMessage.getMessageId().getOffset();
        assertEquals("299", PscCommon.byteArrayToString((byte[]) lastPscMessage.getValue()));
        assertEquals(299, lastConsumedOffset);

        lastKafkaMessage = PscTestUtils.consumeKafkaMessages(100, sharedKafkaTestResource, kafkaCluster, topic3, 0, lastConsumedOffset);
        lastConsumedOffset = lastKafkaMessage.offset();
        assertEquals("399", PscCommon.byteArrayToString((byte[]) lastKafkaMessage.value()));
        assertEquals(399, lastConsumedOffset);
//        lastPscMessage = PscTestUtils.consumePscMessages(100, topicUriPartition, lastConsumedOffset);
//        lastConsumedOffset = lastPscMessage.getMessageId().getOffset();
//        assertEquals("399", PscCommon.byteArrayToString((byte[]) lastPscMessage.getValue()));
//        assertEquals(399, lastConsumedOffset);

        lastPscMessage = PscTestUtils.consumePscMessages(100, topicUriPartition, lastConsumedOffset);
        lastConsumedOffset = lastPscMessage.getMessageId().getOffset();
        assertEquals("499", PscCommon.byteArrayToString((byte[]) lastPscMessage.getValue()));
        assertEquals(499, lastConsumedOffset);
    }

    /**
     * Verifies that the race condition that used to exist in initialization of metric registry and metric reporter of
     * a given key is no longer present.
     *
     * @throws ConfigurationException
     * @throws ConsumerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testMetricRegistryManagerForKeyInitialization() throws ConfigurationException, ConsumerException, InterruptedException {
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        PscMetricRegistryManager metricRegistryManager = PscConsumerUtils.getMetricRegistryManager(pscConsumer);
        PscMetricTag pscMetricTag = new PscMetricTag.Builder().id("id").build();
        AtomicBoolean failed = new AtomicBoolean(false);

        Thread th1 = new Thread(() -> {
            retrieveMetricRegistry(metricRegistryManager, pscMetricTag);
            if (PscMetricsUtil.isInitializationError(metricRegistryManager))
                failed.set(true);
        });

        Thread th2 = new Thread(() -> {
            retrieveMetricRegistry(metricRegistryManager, pscMetricTag);
            if (PscMetricsUtil.isInitializationError(metricRegistryManager))
                failed.set(true);
        });

        th1.start();
        th2.start();
        Thread.sleep(5000);
        th1.join();
        th2.join();

        pscConsumer.close();

        assertFalse("Metrics registry/reporter did not properly get initialized due to a race condition.",
                failed.get()
        );
    }

    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConsumeCompressedMessages() throws SerializerException, InterruptedException {
        int messageCount = 1000;
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1, properties);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1, properties);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1, properties);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        PscTestUtils.produceKafkaMessages(messageCount, sharedKafkaTestResource, kafkaCluster, topic1, properties);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Collections.singleton(topicUriStr1)), 2 * PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(4 * messageCount, messages.size());

        int index = 0;
        for (PscConsumerMessage<String, String> message : messages) {
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            ++index;
        }
        assertEquals(4 * messageCount, index);
    }

    private MetricRegistry retrieveMetricRegistry(PscMetricRegistryManager metricRegistryManager, PscMetricTag pscMetricTag) {
        return metricRegistryManager.getOrCreateMetricRegistry(pscMetricTag, null);
    }
}
