package com.pinterest.psc.integration.consumer;

import com.google.common.collect.Sets;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscBackendConsumer;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.consumer.PscConsumerUtils;
import com.pinterest.psc.consumer.listener.MessageCounterListener;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.producer.SerializerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import io.vavr.control.Either;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestMultiKafkaClusterBackends {
    private static final PscLogger logger = PscLogger.getLogger(TestMultiKafkaClusterBackends.class);

    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource1 = new SharedKafkaTestResource()
            .withBrokers(1).registerListener(new PlainListener().onPorts(9092));
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource2 = new SharedKafkaTestResource()
            .withBrokers(1).registerListener(new PlainListener().onPorts(9093));

    private static final int TEST_TIMEOUT_SECONDS = 60;
    private static final int PSC_CONSUME_TIMEOUT_MS = 10000;
    private static final PscConfiguration pscConfiguration = new PscConfiguration();
    private static String baseClientId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 12;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 24;
    private KafkaCluster kafkaCluster1, kafkaCluster2;
    private String topicUriStr1, topicUriStr2;

    /**
     * Initializes two Kafka clusters that are commonly used by all tests, and creates a single topic on each.
     *
     * @throws IOException
     */
    @BeforeEach
    public void setup() throws IOException, InterruptedException {
        baseClientId = this.getClass().getSimpleName() + "-psc-consumer-client";
        pscConfiguration.clear();
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, this.getClass().getSimpleName() + "-psc-consumer-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_POLL_TIMEOUT_MS, "1000");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        kafkaCluster1 = new KafkaCluster("plaintext", "region", "cluster", 9092);
        topicUriStr1 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster1.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster1.getRegion(), kafkaCluster1.getCluster(), topic1);

        kafkaCluster2 = new KafkaCluster("plaintext", "region2", "cluster2", 9093);
        topicUriStr2 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster2.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster2.getRegion(), kafkaCluster2.getCluster(), topic2);

        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource1, topic1, partitions1);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource2, topic2, partitions2);
        // add a delay to make sure topics are created
        Thread.sleep(1000);
    }

    /**
     * Deleted the topics that are created by default. Also, adds a slight delay to make sure cleanup is complete
     * when tests run consecutively.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource1, topic1);
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource2, topic2);
        // add a delay to make sure topics are deleted
        Thread.sleep(1000);
    }

    /**
     * Verifies that the correct partition counts are returned for the given topic URIs.
     *
     * @throws Exception
     */
    @Test
    public void testGetPartitions() throws Exception {
        PscConsumer pscConsumer = new PscConsumer(pscConfiguration);
        assertEquals(partitions1, pscConsumer.getPartitions(topicUriStr1).size());
        assertEquals(partitions2, pscConsumer.getPartitions(topicUriStr2).size());
        pscConsumer.close();
    }

    /**
     * Uses KafkaProducer to generate a specific number of messages to each topic. Then a PscConsumer subscribes and
     * consumes from both topics from the beginning. The test verifies that the same number of messages are read by the
     * consumer and the messages consumed are as expected (content-wise).
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSubscribeConsumption() throws SerializerException, InterruptedException {
        int messageCount1 = 1000;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource1, kafkaCluster1, topic1);
        int messageCount2 = 2000;
        PscTestUtils.produceKafkaMessages(messageCount2, messageCount1, sharedKafkaTestResource2, kafkaCluster2, topic2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Sets.newHashSet(topicUriStr1, topicUriStr2)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(partitions1 + partitions2, pscConsumerRunner.getAssignment().size());
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount1 + messageCount2, messages.size());

        Set<Integer> idsConsumed = new HashSet<>();
        for (PscConsumerMessage<String, String> message : messages) {
            String topicUriStr = message.getMessageId().getTopicUriPartition().getTopicUriAsString();
            assertTrue(topicUriStr.equals(topicUriStr1) || topicUriStr.equals(topicUriStr2));
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            idsConsumed.add(key);
        }

        assertEquals(messageCount1 + messageCount2, idsConsumed.size());
        int min = idsConsumed.stream().reduce(Integer::min).get();
        int max = idsConsumed.stream().reduce(Integer::max).get();
        assertEquals(0, min);
        assertEquals(messageCount1 + messageCount2 - 1, max);
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
        int messageCount1 = 1000;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource1, kafkaCluster1, topic1);
        int messageCount2 = 2000;
        PscTestUtils.produceKafkaMessages(messageCount2, messageCount1, sharedKafkaTestResource2, kafkaCluster2, topic2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
        PscConsumer<String, String> pscConsumer1 = new PscConsumer<>(pscConfiguration);
        pscConsumer1.subscribe(Arrays.asList(topicUriStr1, topicUriStr2));
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
        PscConsumer<String, String> pscConsumer2 = new PscConsumer<>(pscConfiguration);
        pscConsumer2.subscribe(Arrays.asList(topicUriStr1, topicUriStr2));

        final AtomicInteger countSub = new AtomicInteger(0);
        final AtomicInteger countAll = new AtomicInteger(0);
        PscConsumerPollMessageIterator<String, String> result1;
        PscConsumerPollMessageIterator<String, String> result2;
        while (countSub.get() < messageCount1 + messageCount2 && countAll.get() < messageCount1 + messageCount2) {
            result1 = pscConsumer1.poll();
            result2 = pscConsumer2.poll();

            for (TopicUriPartition topicUriPartition : result1.getTopicUriPartitions()) {
                PscConsumerPollMessageIterator<String, String> iterator1 = result1.iteratorFor(topicUriPartition);
                iterator1.forEachRemaining(message -> countSub.incrementAndGet());
                assertFalse(iterator1.hasNext());
            }
            result1.forEachRemaining(message -> countAll.incrementAndGet());

            for (TopicUriPartition topicUriPartition : result2.getTopicUriPartitions()) {
                PscConsumerPollMessageIterator<String, String> iterator2 = result2.iteratorFor(topicUriPartition);
                iterator2.forEachRemaining(message -> countSub.incrementAndGet());
                assertFalse(iterator2.hasNext());
            }
            result2.forEachRemaining(message -> countAll.incrementAndGet());
        }

        assertEquals(messageCount1 + messageCount2, countSub.get());
        assertEquals(messageCount1 + messageCount2, countAll.get());

        Set<TopicUriPartition> assignment1 = pscConsumer1.assignment();
        Set<TopicUriPartition> assignment2 = pscConsumer2.assignment();
        HashSet<TopicUriPartition> intersection = new HashSet<>(assignment1);
        intersection.retainAll(assignment2);
        assertTrue(intersection.isEmpty());
        HashSet<TopicUriPartition> union = new HashSet<>(assignment1);
        union.addAll(assignment2);
        assertEquals(partitions1 + partitions2, union.size());

        pscConsumer1.close();
        pscConsumer2.close();
    }

    /**
     * Verifies message consumption works as expected when consumer calls subscribe() multiple times to consume
     * from different topics.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConsecutiveSubscribeConsumption() throws SerializerException, InterruptedException {
        int messageCount1 = 1000;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource1, kafkaCluster1, topic1);

        int messageCount2 = 2000;
        PscTestUtils.produceKafkaMessages(messageCount2, messageCount1, sharedKafkaTestResource2, kafkaCluster2, topic2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        // consume from first topic
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Sets.newHashSet(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(partitions1, pscConsumerRunner.getAssignment().size());
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount1, messages.size());

        Set<Integer> idsConsumed = new HashSet<>();
        for (PscConsumerMessage<String, String> message : messages) {
            String topicUriStr = message.getMessageId().getTopicUriPartition().getTopicUriAsString();
            assertEquals(topicUriStr1, topicUriStr);
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            idsConsumed.add(key);
        }
        assertEquals(messageCount1, idsConsumed.size());
        int min = idsConsumed.stream().reduce(Integer::min).get();
        int max = idsConsumed.stream().reduce(Integer::max).get();
        assertEquals(0, min);
        assertEquals(messageCount1 - 1, max);

        // consume from second topic
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
        pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.left(Sets.newHashSet(topicUriStr2)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(partitions2, pscConsumerRunner.getAssignment().size());
        messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount2, messages.size());

        idsConsumed.clear();
        for (PscConsumerMessage<String, String> message : messages) {
            String topicUriStr = message.getMessageId().getTopicUriPartition().getTopicUriAsString();
            assertEquals(topicUriStr2, topicUriStr);
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            idsConsumed.add(key);
        }
        assertEquals(messageCount2, idsConsumed.size());
        min = idsConsumed.stream().reduce(Integer::min).get();
        max = idsConsumed.stream().reduce(Integer::max).get();
        assertEquals(messageCount1, min);
        assertEquals(messageCount1 + messageCount2 - 1, max);
    }

    /**
     * Uses KafkaProducer to generate a specific number of message to a single partition of each topic. The test
     * verifies that a PscConsumer would not consume any message from another partition except from the ones to
     * which messages were produced. It also confirms that messages consumed are as expected (content-wise).
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testAssignConsumption() throws SerializerException, InterruptedException, TopicUriSyntaxException {
        int messageCount1 = 1000;
        int targetPartition1 = 1;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource1, kafkaCluster1, topic1, targetPartition1);

        int messageCount2 = 2000;
        int targetPartition2 = 2;
        PscTestUtils.produceKafkaMessages(messageCount2, messageCount1, sharedKafkaTestResource2, kafkaCluster2, topic2, targetPartition2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        Set<TopicUriPartition> emptyPartitions = new HashSet<>();
        Set<TopicUriPartition> nonEmptyPartitions = new HashSet<>();
        for (int i = 0; i < partitions1; ++i) {
            TopicUriPartition topicUriPartition = TestUtils.getFinalizedTopicUriPartition(
                    KafkaTopicUri.validate(TopicUri.validate(topicUriStr1)), i
            );
            if (i == targetPartition1)
                nonEmptyPartitions.add(topicUriPartition);
            else
                emptyPartitions.add(topicUriPartition);
        }
        for (int i = 0; i < partitions2; ++i) {
            TopicUriPartition topicUriPartition = TestUtils.getFinalizedTopicUriPartition(
                    KafkaTopicUri.validate(TopicUri.validate(topicUriStr2)), i
            );
            if (i == targetPartition2)
                nonEmptyPartitions.add(topicUriPartition);
            else
                emptyPartitions.add(topicUriPartition);
        }

        // first try with partitions we did not produce to
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(emptyPartitions), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(emptyPartitions, pscConsumerRunner.getAssignment());
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(0, messages.size());

        // now try the partitions with messages
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
        pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration, Either.right(nonEmptyPartitions), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        assertEquals(nonEmptyPartitions, pscConsumerRunner.getAssignment());
        messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount1 + messageCount2, messages.size());

        int index1 = 0;
        int index2 = messageCount1;
        for (PscConsumerMessage<String, String> message : messages) {
            String topicUriStr = message.getMessageId().getTopicUriPartition().getTopicUriAsString();
            int partition = message.getMessageId().getTopicUriPartition().getPartition();
            long offset = message.getMessageId().getOffset();
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            if (topicUriStr.equals(topicUriStr1)) {
                assertEquals(targetPartition1, partition);
                assertEquals(index1, offset);
                assertEquals(index1++, key);
            } else if (topicUriStr.equals(topicUriStr2)) {
                assertEquals(targetPartition2, partition);
                assertEquals(index2 - messageCount1, offset);
                assertEquals(index2++, key);
            } else
                fail("Message was consumed from an unexpected topic " + topicUriStr);
        }
        assertEquals(messageCount1, index1);
        assertEquals(messageCount2, index2 - messageCount1);
    }

    /**
     * Verifies message consumption works as expected when consumer calls assign() multiple times to consume
     * from different partitions.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConsecutiveAssignConsumption() throws SerializerException, InterruptedException {
        int messageCount1 = 1000;
        int targetPartition1 = 1;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource1, kafkaCluster1, topic1, targetPartition1);

        int messageCount2 = 2000;
        int targetPartition2 = 2;
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource2, kafkaCluster2, topic2, targetPartition2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        // first consumer from one partition
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration,
                Either.right(Collections.singleton(new TopicUriPartition(topicUriStr1, targetPartition1))),
                PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        List<PscConsumerMessage<String, String>> messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount1, messages.size());

        int index1 = 0;
        for (PscConsumerMessage<String, String> message : messages) {
            int partition = message.getMessageId().getTopicUriPartition().getPartition();
            long offset = message.getMessageId().getOffset();
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            assertEquals(targetPartition1, partition);
            assertEquals(index1, offset);
            assertEquals(index1++, key);
        }
        assertEquals(messageCount1, index1);

        // then consumer from the second partition
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseClientId + "-" + UUID.randomUUID());
        pscConsumerRunner = new PscConsumerRunner<>(
                pscConfiguration,
                Either.right(Collections.singleton(new TopicUriPartition(topicUriStr2, targetPartition2))),
                PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoffAndWaitForCompletion();
        messages = pscConsumerRunner.getMessages();
        assertEquals(messageCount2, messages.size());

        index1 = 0;
        for (PscConsumerMessage<String, String> message : messages) {
            int partition = message.getMessageId().getTopicUriPartition().getPartition();
            long offset = message.getMessageId().getOffset();
            int key = Integer.parseInt(message.getKey());
            int value = Integer.parseInt(message.getValue());
            assertEquals(value, key);
            assertEquals(targetPartition2, partition);
            assertEquals(index1, offset);
            assertEquals(index1++, key);
        }
        assertEquals(messageCount2, index1);
    }

    /**
     * Verifies that a configured listener works as expected and processes every message. Also, it makes sure that
     * when listener is configured, using PSC consumer poll() is disallowed.
     *
     * @throws Exception
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testListener() throws Exception {
        int messageCount1 = 1000;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource1, kafkaCluster1, topic1);

        int messageCount2 = 2000;
        PscTestUtils.produceKafkaMessages(messageCount2, sharedKafkaTestResource2, kafkaCluster2, topic2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_MESSAGE_LISTENER, MessageCounterListener.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Sets.newHashSet(topicUriStr1, topicUriStr2));
        Exception e = assertThrows(ConsumerException.class, pscConsumer::poll);
        assertEquals(ExceptionMessage.MUTUALLY_EXCLUSIVE_APIS("poll()", "MessageListener"), e.getMessage());
        Thread.sleep(2 * PSC_CONSUME_TIMEOUT_MS);
        pscConsumer.unsubscribe();
        pscConsumer.close();
        assertEquals(messageCount1 + messageCount2,
                ((MessageCounterListener) pscConsumer.getListener()).getHandleCallCounter()
        );
    }

    /**
     * Verifies that consumer wakeup works as expected. It launches a PSC consumer that relies on two backend consumers,
     * where at least one of those consumers gets stuck into a poll() call due to large timeout. Then it verifies that
     * calling wakeup() on PSC consumer actually interrupts both backend consumers and as a result interrupts the PSC
     * consumer.
     *
     * @throws InterruptedException
     * @throws SerializerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testWakeup() throws InterruptedException, SerializerException {
        PscTestUtils.produceKafkaMessages(1000, sharedKafkaTestResource1, kafkaCluster1, topic1);
        PscTestUtils.produceKafkaMessages(0, sharedKafkaTestResource2, kafkaCluster2, topic2);

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
     * Verifies that resubscription of PscConsumers to various topic URIs on different backends works fine.
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testMultiSubscription() throws ConsumerException, ConfigurationException {
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Sets.newHashSet(topicUriStr1));
        pscConsumer.subscribe(Sets.newHashSet(topicUriStr1, topicUriStr2));
        pscConsumer.subscribe(Sets.newHashSet(topicUriStr2));
        pscConsumer.subscribe(Sets.newHashSet(topicUriStr1));
        pscConsumer.subscribe(Sets.newHashSet());
        pscConsumer.close();
    }

    /**
     * Verifies that the PSC consumer preserves metrics emitted from the backend consumer.
     *
     * @throws ConfigurationException
     * @throws ClientException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testPreservationOfBackendMetrics() throws ConfigurationException, ClientException {
        int messageCount1 = 1000;
        PscTestUtils.produceKafkaMessages(messageCount1, sharedKafkaTestResource1, kafkaCluster1, topic1);
        int messageCount2 = 2000;
        PscTestUtils.produceKafkaMessages(messageCount2, messageCount1, sharedKafkaTestResource2, kafkaCluster2, topic2);

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());

        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Arrays.asList(topicUriStr1, topicUriStr2));
        int count = 0;
        while (true) {
            PscConsumerPollMessageIterator<String, String> messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                messageIterator.next();
                ++count;
            }

            if (count >= messageCount1 + messageCount2)
                break;
        }

        int backendMetrics = 0;
        for (PscBackendConsumer<String, String> backendConsumer : PscConsumerUtils.getBackendConsumersOf(pscConsumer)) {
            backendMetrics += backendConsumer.metrics().keySet().size();
        }

        Map<MetricName, Metric> metrics = pscConsumer.metrics();
        assertEquals(backendMetrics, metrics.keySet().size());
        pscConsumer.close();
    }
}
