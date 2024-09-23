package com.pinterest.psc.integration.producer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.integration.consumer.PscConsumerRunner;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscBackendProducer;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.producer.PscProducerTransactionalProperties;
import com.pinterest.psc.producer.PscProducerUtils;
import com.pinterest.psc.producer.creation.PscKafkaProducerCreator;
import com.pinterest.psc.serde.ByteArraySerializer;
import com.pinterest.psc.serde.IntegerDeserializer;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.serde.StringSerializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.vavr.control.Either;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOneKafkaBackend {
    private static final PscLogger logger = PscLogger.getLogger(TestOneKafkaBackend.class);
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokers(1)
            .withBrokerProperty("auto.create.topics.enable", "false");

    private static final int TEST_TIMEOUT_SECONDS = 60;
    private static final int PSC_CONSUME_TIMEOUT_MS = 7500;
    private static final PscConfiguration producerConfiguration = new PscConfiguration();
    private static final PscConfiguration consumerConfiguration = new PscConfiguration();
    private static String baseProducerClientId;
    private static String baseConsumerClientId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 12;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 1;
    private static final String topic3 = "topic3";
    private static final int partitions3 = 1;
    private KafkaCluster kafkaCluster;
    private String topicUriStr1, topicUriStr2, topicUriStr3;

    /**
     * Initializes a Kafka cluster that is commonly used by all tests, and creates a single topic on it.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeEach
    public void setup() throws IOException, InterruptedException {
        baseProducerClientId = this.getClass().getSimpleName() + "-psc-producer-client";
        producerConfiguration.clear();
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, baseProducerClientId + "-" + UUID.randomUUID());
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");

        baseConsumerClientId = this.getClass().getSimpleName() + "-psc-consumer-client";
        consumerConfiguration.clear();
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseConsumerClientId + "-" + UUID.randomUUID());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");

        int port = sharedKafkaTestResource.getKafkaTestUtils().describeClusterNodes().iterator().next().port();
        kafkaCluster = new KafkaCluster("plaintext", "region", "cluster", port);
        topicUriStr1 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), topic1);
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
        Thread.sleep(1000);
    }

    /**
     * Verifies that producer send properly published messages to target topic URI (with random partitioning).
     *
     * @throws ProducerException
     * @throws ConfigurationException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSimpleProducer() throws ProducerException, ConfigurationException, ExecutionException, InterruptedException, IOException {
        int messageCount = 1000;
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);
        List<Future<MessageId>> sendResponseFutures = new ArrayList<>();
        PscProducerMessage<String, String> pscProducerMessage;
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<>(topicUriStr1, "" + i, "" + i, System.currentTimeMillis());
            sendResponseFutures.add(pscProducer.send(pscProducerMessage));
        }

        List<List<MessageId>> messageIdsByPartition = new ArrayList<>();
        for (int i = 0; i < partitions1; ++i)
            messageIdsByPartition.add(new ArrayList<>());

        for (Future<MessageId> future : sendResponseFutures) {
            MessageId messageId = future.get();
            int partition = messageId.getTopicUriPartition().getPartition();
            assertTrue(0 <= partition && partition < partitions1);
            messageIdsByPartition.get(partition).add(messageId);
        }

        for (List<MessageId> messageIds : messageIdsByPartition) {
            int offset = 0;
            for (MessageId messageId : messageIds)
                assertEquals(offset++, messageId.getOffset());
        }

        pscProducer.close();
    }

    /**
     * Verifies that the producer send callback is triggered once send execution completes.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSimpleProducerWithCallback() throws ProducerException, ConfigurationException, InterruptedException, IOException {
        int messageCount = 100;
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);
        SimpleSendCallback callback = new SimpleSendCallback();

        List<Future<MessageId>> sendResponseFutures = new ArrayList<>();
        PscProducerMessage<String, String> pscProducerMessage;
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<>(topicUriStr1, "" + i, "" + i, System.currentTimeMillis());
            sendResponseFutures.add(pscProducer.send(pscProducerMessage, callback));
        }

        boolean completed = false;
        while (!completed) {
            completed = true;
            for (Future<MessageId> future : sendResponseFutures) {
                if (!future.isDone()) {
                    completed = false;
                    break;
                }
                Thread.sleep(20);
            }
        }

        assertEquals(messageCount, callback.getCompletionCallCount());
        pscProducer.close();
    }

    /**
     * Verifies that the correct partition count is returned for the given topic URI.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testGetPartitions() throws ProducerException, ConfigurationException, IOException {
        PscProducer<byte[], byte[]> pscProducer = new PscProducer<>(producerConfiguration);
        assertEquals(partitions1, pscProducer.getPartitions(topicUriStr1).size());
        pscProducer.close();
    }

    /**
     * Verifies that calling flush ensures all queued messages are delivered.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testFlushWithCloseTimeout() throws ProducerException, ConfigurationException, IOException {
        long messageCount = 10_000;
        assertEquals(messageCount, produceAndCloseAndCheckCompletedCount(messageCount, true, 0L));
    }

    /**
     * Verifies that calling close with no timeout ensures all queued messages are delivered first.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCloseNoTimeout() throws ProducerException, ConfigurationException, IOException {
        long messageCount = 10_000;
        assertEquals(messageCount, produceAndCloseAndCheckCompletedCount(messageCount, false, null));
    }

    /**
     * Verifies that calling close with a timeout too soon will cause incomplete messages in the queue
     * to drop.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testCloseWithTimeout() throws ProducerException, ConfigurationException, IOException {
        long messageCount = 10_000;
        assertTrue(produceAndCloseAndCheckCompletedCount(messageCount, false, 0L) < messageCount);
    }

    /**
     * Verifies that backend producers are created in isolation when multiple PSC producers are used.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testBackendProducerIsolationAcrossPscProducers() throws ProducerException, ConfigurationException, IOException {
        PscProducer<byte[], byte[]> pscProducer1 = new PscProducer<>(producerConfiguration);
        PscProducer<byte[], byte[]> pscProducer2 = new PscProducer<>(producerConfiguration);

        pscProducer1.getPartitions(topicUriStr1);
        pscProducer2.getPartitions(topicUriStr1);

        Collection<PscBackendProducer> backendProducers1 = PscProducerUtils.getBackendProducersOf(pscProducer1);
        Collection<PscBackendProducer> backendProducers2 = PscProducerUtils.getBackendProducersOf(pscProducer2);

        assertNotNull(backendProducers1);
        assertNotNull(backendProducers2);
        assertEquals(1, backendProducers1.size());
        assertEquals(1, backendProducers2.size());
        assertNotEquals(backendProducers1.iterator().next(), backendProducers2.iterator().next());

        pscProducer1.close();
        pscProducer2.close();
    }

    /**
     * Verified than a transaction id is required for enabling transactions against a Kafka backend. Also,
     * verifies that automated configuration override (acks=1 -> acks=all) occurs when necessary.
     *
     * @throws ConfigurationException
     * @throws ProducerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testTransactionalProducerConfiguration() throws ConfigurationException, ProducerException, IOException {
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(producerConfiguration);
        pscProducer.beginTransaction();
        PscProducerMessage<Integer, Integer> producerMessage = new PscProducerMessage<>(
                topicUriStr1,
                0
        );
        Exception e = assertThrows(ProducerException.class, () -> pscProducer.send(producerMessage));
        assertEquals(IllegalStateException.class, e.getCause().getClass());
        pscProducer.close();

        // add transactional id, even though ack=1 PscKafkaProducer should overwrite it
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, "test-transactional-id");
        PscProducer<Integer, Integer> pscProducer2 = new PscProducer<>(producerConfiguration);
        pscProducer2.beginTransaction();
        PscProducerMessage<Integer, Integer> producerMessage2 = new PscProducerMessage<>(
                topicUriStr1,
                0
        );
        pscProducer2.send(producerMessage2);
        pscProducer2.close();
    }

    /**
     * Verifies that non-committed messages are not consumed by transactional consumers
     *
     * @throws ConfigurationException
     * @throws ProducerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testTransactionalProducerWithNoCommit() throws ConfigurationException, ProducerException, InterruptedException, IOException {
        int messageCount = 100;
        produceTransactionally(producerConfiguration, messageCount, CommitOrAbort.NONE);

        // no messages should have been committed (so a transacational consumer should not read anything)
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL, PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL_TRANSACTIONAL);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "try1");
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                consumerConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(partitions1, pscConsumerRunner.getAssignment().size());
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(0, messages.size());

        // with read_uncommitted all messages should be read
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL, PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL_NON_TRANSACTIONAL);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "try2");
        pscConsumerRunner = new PscConsumerRunner<>(
                consumerConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(partitions1, pscConsumerRunner.getAssignment().size());
        messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount, messages.size());
    }

    /**
     * Verifies that a transactional PSC producer that has sent messages cannot send messages in a non-transactional way.
     *
     * @throws ConfigurationException
     * @throws ProducerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testTransactionalProducersWithIncorrectTransactionalCallOrder() throws ConfigurationException, ProducerException, IOException {
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, "test-transactional-id");

        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(producerConfiguration);
        pscProducer.beginTransaction();
        PscProducerMessage<Integer, Integer> producerMessage = new PscProducerMessage<>(topicUriStr1, 0);
        pscProducer.send(producerMessage);
        pscProducer.commitTransaction();

        Exception e = assertThrows(ProducerException.class, () -> pscProducer.commitTransaction());
        assertEquals("Invalid transaction state: call to commitTransaction() before transaction begun.", e.getMessage());
        pscProducer.close();
    }

    /**
     * Verifies that a transactional PSC producer that hasn't sent messages yet cannot send messages in a non-transactional way.
     *
     * @throws ConfigurationException
     * @throws ProducerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testTransactionalProducerWithKafkaBackendCannotBeNonTransactional() throws ConfigurationException, ProducerException, IOException {
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, "test-transactional-id");
        producerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "false");

        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(producerConfiguration);
        PscProducerMessage<Integer, Integer> producerMessage = new PscProducerMessage<>(topicUriStr1, 0);
        Exception e = assertThrows(ProducerException.class, () -> pscProducer.send(producerMessage));
        assertEquals(IllegalStateException.class, e.getCause().getClass());
        pscProducer.close();
    }

    /**
     * Verifies that the <code>initTransactions()</code> API works as expected.
     *
     * @throws ConfigurationException
     * @throws ProducerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testInitTransactions() throws ConfigurationException, ProducerException, IOException {
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        String transactionId = "test-transactional-id-" + UUID.randomUUID();
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, transactionId);

        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(producerConfiguration);
        PscProducerUtils.initTransactions(pscProducer, topicUriStr1);
        Collection<PscBackendProducer> backendProducers = PscProducerUtils.getBackendProducersOf(pscProducer);
        assertEquals(1, backendProducers.size());
        PscBackendProducer backendProducer = backendProducers.iterator().next();
        PscProducerTransactionalProperties transactionalProperties = backendProducer.getTransactionalProperties();
        long producerId = transactionalProperties.getProducerId();
        assertEquals(0, transactionalProperties.getEpoch());

        // cannot initialize the same producer twice
//        Exception e = assertThrows(ProducerException.class, () -> PscProducerUtils.initTransactions(pscProducer, topicUriStr1));
//        assertEquals("Invalid transaction state: initializing transactions works only once for a PSC producer.", e.getMessage());
//        pscProducer.abortTransaction();
//        pscProducer.close();

        // but possible to initialize a second producer with the same transaction id
        PscProducer<Integer, Integer> pscProducer2 = new PscProducer<>(producerConfiguration);
        PscProducerUtils.initTransactions(pscProducer2, topicUriStr1);
        backendProducers = PscProducerUtils.getBackendProducersOf(pscProducer2);
        assertEquals(1, backendProducers.size());
        backendProducer = backendProducers.iterator().next();
        transactionalProperties = backendProducer.getTransactionalProperties();
        // it should bump the epoch each time for the same producer id
        assertEquals(producerId, transactionalProperties.getProducerId());
        assertEquals(1, transactionalProperties.getEpoch());
        pscProducer2.abortTransaction();
        pscProducer2.close();

        // over and over again
        PscProducer<Integer, Integer> pscProducer3 = new PscProducer<>(producerConfiguration);
        PscProducerUtils.initTransactions(pscProducer3, topicUriStr1);
        backendProducers = PscProducerUtils.getBackendProducersOf(pscProducer3);
        assertEquals(1, backendProducers.size());
        backendProducer = backendProducers.iterator().next();
        transactionalProperties = backendProducer.getTransactionalProperties();
        assertEquals(producerId, transactionalProperties.getProducerId());
        assertEquals(2, transactionalProperties.getEpoch());
        pscProducer3.abortTransaction();
        pscProducer3.close();
    }

    /**
     * Verifies that in case of a non-existing topic URI the <code>getPartition()</code> call returns proper result.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testGetPartitionsOfNonExistingTopicUri() throws ProducerException, ConfigurationException, IOException {
        producerConfiguration.setProperty(PscConfiguration.PSC_AUTO_RESOLUTION_ENABLED, "false");
        producerConfiguration.setProperty("psc.producer.max.block.ms", "3000");
        PscProducer<byte[], byte[]> pscProducer = new PscProducer<>(producerConfiguration);
        String topicUriStr = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), "not_there");
        Exception e = assertThrows(ProducerException.class, () -> pscProducer.getPartitions(topicUriStr));
        assertEquals(TimeoutException.class, e.getCause().getClass());
        pscProducer.close();
    }

    /**
     * Verifies that resetting a non-transactional backend producer works as expected.
     *
     * @throws ConfigurationException
     * @throws ProducerException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testResetBackendProducerOfNonTransactionalPscProducer() throws ConfigurationException, ProducerException, ExecutionException, InterruptedException, IOException {
        int messageCount = 10;
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty("psc.producer.request.timeout.ms", "3000");
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);

        // do an initial set of sends
        PscProducerMessage<String, String> pscProducerMessage;
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<>(topicUriStr1, "" + i, "" + i, System.currentTimeMillis());
            pscProducer.send(pscProducerMessage);
        }

        assertEquals(1, PscProducerUtils.getBackendProducersOf(pscProducer).size());
        PscBackendProducer backendProducer1 = PscProducerUtils.getBackendProducersOf(pscProducer).iterator().next();

        // reset backend producer
        PscProducerUtils.resetBackendProducer(pscProducer, backendProducer1);

        // do more sends and verify a new backend producer is successfully producing
        Future<MessageId> future;
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<>(topicUriStr1, "" + i, "" + i, System.currentTimeMillis());
            future = pscProducer.send(pscProducerMessage);
            assertNotNull(future);
            MessageId messageId = future.get();
            assertNotNull(messageId);
            assertNotNull(messageId.getTopicUriPartition());
            assertTrue(messageId.getOffset() >= 0);
        }

        assertEquals(1, PscProducerUtils.getBackendProducersOf(pscProducer).size());
        PscBackendProducer backendProducer2 = PscProducerUtils.getBackendProducersOf(pscProducer).iterator().next();
        assertNotEquals(backendProducer1, backendProducer2);

        pscProducer.close();
    }

    /**
     * Verifies that sending offsets to transactions works as expected. The test involves a consumer and transactional
     * producer pair that consume messages, "process" them, and then produce (transformed) messages. The consumer
     * offsets should not be committed until the producer commits.
     *
     * @throws ConfigurationException
     * @throws ProducerException
     * @throws ConsumerException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSendOffsetsToTransaction() throws ConfigurationException, ProducerException, ConsumerException, ExecutionException, InterruptedException, IOException {
        // first bootstrap the source single-partition topic by sending 100 messages
        int messageCount = 100;
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(producerConfiguration);

        String consumerGroup = "consumer-group";
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, IntegerDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, IntegerDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, consumerGroup);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, 1);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        PscConsumer<Integer, Integer> pscConsumer = new PscConsumer<>(consumerConfiguration);

        PscProducerMessage<Integer, Integer> pscProducerMessage;
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<Integer, Integer>(topicUriStr2, i, i, System.currentTimeMillis());
            pscProducer.send(pscProducerMessage);
        }
        pscProducer.flush();
        pscProducer.close();

        // validate messages are in topic
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUriStr2, 0);
        assertEquals(100L, (long) pscConsumer.endOffsets(Collections.singleton(topicUriPartition)).get(topicUriPartition));

        // now, read 10 messages from the start
        pscConsumer.subscribe(Collections.singleton(topicUriStr2));
        List<PscConsumerMessage<Integer, Integer>> consumed = new ArrayList<>();
        MessageId messageId = null;
        while (true) {
            PscConsumerPollMessageIterator<Integer, Integer> messages = pscConsumer.poll();
            while (messages.hasNext()) {
                PscConsumerMessage<Integer, Integer> message = messages.next();
                consumed.add(message);
                messageId = message.getMessageId();
            }
            if (consumed.size() == 10)
                break;
        }

        // verify that consumer position has changed, but no offset is committed as the consumer has not committed.
        assertNull(pscConsumer.committed(topicUriPartition));
        assertEquals(10L, pscConsumer.position(topicUriPartition));

        // now use a transactional producer to offload (transformed) messages to a second topic
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, "transaction-01");
        PscProducer<Integer, Integer> transactionalProducer = new PscProducer<>(producerConfiguration);

        transactionalProducer.beginTransaction();
        for (PscConsumerMessage<Integer, Integer> message : consumed) {
            PscProducerMessage<Integer, Integer> transformed = new PscProducerMessage<>(
                    topicUriStr3, message.getKey() * 10, message.getValue() * 10);
            transactionalProducer.send(transformed);
        }

        // verify that producer hasn't committed and neither has the original consumer
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL, PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL_TRANSACTIONAL);
        PscConsumer<Integer, Integer> transactionalConsumer = new PscConsumer<>(consumerConfiguration);
        TopicUriPartition targetTopicUriPartition = new TopicUriPartition(topicUriStr3, 0);
        assertEquals(0L, (long) transactionalConsumer.endOffsets(Collections.singleton(targetTopicUriPartition)).get(targetTopicUriPartition));
        assertNull(pscConsumer.committed(topicUriPartition));

        // now send offsets to transaction
        Map<TopicUriPartition, MessageId> offsetsToSend = new HashMap<>();
        offsetsToSend.put(topicUriPartition, messageId);
        transactionalProducer.sendOffsetsToTransaction(offsetsToSend, consumerGroup);

        // verify that still producer hasn't committed and neither has the original consumer
        assertEquals(0L, (long) transactionalConsumer.endOffsets(Collections.singleton(targetTopicUriPartition)).get(targetTopicUriPartition));
        // TODO: figure out why the below call times out
//        assertNull(pscConsumer.committed(topicUriPartition));

        // now producer commits
        transactionalProducer.commitTransaction();
        transactionalProducer.close();
        Thread.sleep(1000);

        // verify that producer has committed and original consumer has too
        // 11 = 10 messages + 1 commit marker
        assertEquals(11L, (long) transactionalConsumer.endOffsets(Collections.singleton(targetTopicUriPartition)).get(targetTopicUriPartition));
        MessageId committedMessageId = pscConsumer.committed(topicUriPartition);
        assertNotNull(committedMessageId);
        // 9 = last committed offset
        assertEquals(9L, committedMessageId.getOffset());

        pscConsumer.close();
        transactionalConsumer.close();
    }

    /**
     * Verifies that backend producer creation/extraction works as expected; i.e. the correct backend producer is
     * returned given the PSC producer configuration.
     *
     * @throws TopicUriSyntaxException
     * @throws ConfigurationException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testProducerCreator() throws TopicUriSyntaxException, ConfigurationException {
        PscKafkaProducerCreator<String, String> creator = new PscKafkaProducerCreator<>();
        TopicUri topicUri = TopicUri.validate("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:");
        TopicUri kafkaTopicUri = KafkaTopicUri.validate(topicUri);

        PscConfigurationInternal pscConfigurationInternal = new PscConfigurationInternal(producerConfiguration, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);

        Set<PscBackendProducer<String, String>> producers = creator.getProducers(
                pscConfigurationInternal.getEnvironment(),
                pscConfigurationInternal,
                null,
                Collections.singleton(kafkaTopicUri)
        );

        producers.forEach(p -> {
            PscConfiguration configuration = p.getConfiguration();
            Assertions.assertEquals(kafkaCluster.getBootstrap(),
                    configuration.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            Assertions.assertEquals(org.apache.kafka.common.serialization.ByteArraySerializer.class.getName(),
                    configuration.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
            Assertions.assertEquals(org.apache.kafka.common.serialization.ByteArraySerializer.class.getName(),
                    configuration.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            Assertions.assertEquals("PLAINTEXT",
                    configuration.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
            try {
                p.close();
            } catch (Exception exception) {
                // OK
            }
        });
    }

    private long produceAndCloseAndCheckCompletedCount(long messageCount, boolean flush, Long closeTimeout) throws ProducerException, ConfigurationException, IOException {
        // set up Kafka consumer for checking end offsets after producer is done
        KafkaConsumer<byte[], byte[]> kafkaConsumer = sharedKafkaTestResource.getKafkaTestUtils().getKafkaConsumer(
                ByteArrayDeserializer.class,
                ByteArrayDeserializer.class
        );
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < partitions1; ++i)
            topicPartitions.add(new TopicPartition(topic1, i));

        // set up PSC producer and send messages
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
        PscProducer<byte[], byte[]> pscProducer = new PscProducer<>(producerConfiguration);
        byte[] b = new byte[1024];
        Random random = new Random();
        for (int i = 0; i < messageCount; ++i) {
            random.nextBytes(b);
            pscProducer.send(new PscProducerMessage<>(topicUriStr1, b));
        }

        if (flush)
            pscProducer.flush();

        if (closeTimeout == null)
            pscProducer.close();
        else
            pscProducer.close(Duration.ofMillis(closeTimeout));

        // check how many messages were successfully sent to the topic
        long producedCount = kafkaConsumer.endOffsets(topicPartitions).values().stream().mapToLong(offset -> offset).sum();
        kafkaConsumer.close();

        return producedCount;
    }

    private void produceTransactionally(PscConfiguration pscConfiguration, long messageCount, CommitOrAbort commitOrAbort) throws ProducerException, ConfigurationException, IOException {
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, "test-transaction-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_ACKS, "-1");
        PscProducer<byte[], byte[]> pscProducer = new PscProducer<>(pscConfiguration);

        pscProducer.beginTransaction();

        byte[] b = new byte[1024];
        Random random = new Random();
        for (int i = 0; i < messageCount; ++i) {
            random.nextBytes(b);
            pscProducer.send(new PscProducerMessage<>(topicUriStr1, b));
        }

        switch (commitOrAbort) {
            case NONE:
                break;
            case COMMIT:
                pscProducer.commitTransaction();
                break;
            case ABORT:
                pscProducer.abortTransaction();
                break;
        }

        pscProducer.close();
    }

    private enum CommitOrAbort {
        NONE,
        COMMIT,
        ABORT
    }

    ;

    static class SimpleSendCallback implements Callback {
        private int completionCallCount = 0;

        @Override
        public void onCompletion(MessageId messageId, Exception exception) {
            ++completionCallCount;
        }

        public int getCompletionCallCount() {
            return completionCallCount;
        }
    }
}
