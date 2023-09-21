package com.pinterest.psc.integration.consumer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.OffsetCommitCallback;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.serde.StringDeserializer;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestApiBasics {
    private static final PscLogger logger = PscLogger.getLogger(TestApiBasics.class);

    private static final PscConfiguration pscConfiguration = new PscConfiguration();
    private KafkaCluster kafkaCluster;
    private static final String topic1 = "topic1";
    private static final String topic2 = "topic2";
    private String topicUriStr1, topicUriStr2;

    @BeforeEach
    public void setup() throws IOException {
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-client");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        kafkaCluster = new KafkaCluster("plaintext", "region", "cluster", 9092);
        topicUriStr1 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), topic1);
        topicUriStr2 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster.getRegion(), kafkaCluster.getCluster(), topic2);
    }

    /**
     * Verifies that most public APIs are PscConsumer are accessible via a single thread.
     *
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws ConsumerException
     */
    @Test
    public void testPscConsumerThreadSafety() throws ConfigurationException, InterruptedException, ConsumerException {
        AtomicBoolean consumerApiCalled = new AtomicBoolean(false);
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUriStr1, 0);
        Thread longRunningThread = new Thread(() -> {
            try {
                logger.info("Long running thread id: {}", Thread.currentThread().getId());
                pscConsumer.subscribe(Collections.singleton(topicUriStr1));
                consumerApiCalled.set(true);
                pscConsumer.poll(Duration.ofMinutes(10));
            } catch (ConsumerException | ConfigurationException e) {
                e.printStackTrace();
            } catch (InterruptException e) {
                // OK
            }
        });

        AtomicBoolean allPassed = new AtomicBoolean(false);
        //invoke an API to register the consumer thread
        Thread anotherThread = new Thread(() -> {
            try {
                logger.info("Another thread id: {}", Thread.currentThread().getId());
                while (!consumerApiCalled.get());
                // wait to make sure the other thread has called poll().
                Thread.sleep(1000);
                List<Exception> exceptions = new ArrayList<>();
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.assign(Collections.EMPTY_SET)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.assignment()));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.close()));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.commitSync((MessageId) null)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.commitSync(Collections.EMPTY_SET)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.commitAsync((OffsetCommitCallback) null)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.commitAsync(Collections.EMPTY_SET)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.commitAsync(Collections.EMPTY_SET, null)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.committed(topicUriPartition)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.endOffsets(Collections.singleton(topicUriPartition))));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.getMessageIdByTimestamp(Collections.EMPTY_MAP)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.getPartitions(topicUriStr1)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.poll()));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.poll(Duration.ZERO)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.position(topicUriPartition)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seek(topicUriStr1, 0)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seekToTimestamp(topicUriStr1, 0)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seek(topicUriPartition, 0)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seekToTimestamp(topicUriPartition, 0)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seek((MessageId) null)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seek(Collections.EMPTY_SET)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seek(Collections.EMPTY_MAP)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seekToTimestamp(Collections.EMPTY_MAP)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seekToBeginning(Collections.singleton(topicUriPartition))));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.seekToEnd(Collections.singleton(topicUriPartition))));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.subscribe(Collections.EMPTY_SET)));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.subscription()));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.unassign()));
                exceptions.add(assertThrows(ConcurrentModificationException.class, () -> pscConsumer.unsubscribe()));

                exceptions.forEach(e -> assertEquals(ExceptionMessage.MULTITHREADED_EXCEPTION, e.getMessage()));
                allPassed.set(true);
            } catch (Exception e) {
                fail("Unexpected exception when calling PscConsumer APIs: " + e.getMessage());
            }
        });
        longRunningThread.start();
        anotherThread.start();
        anotherThread.join();
        Thread.sleep(2000);
        longRunningThread.interrupt();
        longRunningThread.join();
        pscConsumer.close();
        if (!allPassed.get())
            fail("Not all thread safety checks passed.");
    }

    /**
     * Verifies that partition assignment of a PSC consumer is as expected.
     *
     * @throws Exception
     */
    @Test
    public void testAssignment() throws Exception {
        int targetPartition = 0;
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUriStr1, targetPartition);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.assign(Collections.singleton(topicUriPartition));
        assertEquals(Collections.singleton(topicUriPartition), pscConsumer.assignment());

        // invalid topic uri
        TopicUriPartition topicUriPartition2 = new TopicUriPartition("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":backend:env:cloud_region::cluster:topic1", targetPartition);
        Exception e = assertThrows(ConsumerException.class,
                () -> pscConsumer.assign(Collections.singleton(topicUriPartition2))
        );
        assertEquals(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND("backend"), e.getMessage());

        pscConsumer.unassign();
        pscConsumer.close();
    }

    /**
     * Verifies that topic subscription of a PSC consumer is as expected.
     *
     * @throws Exception
     */
    @Test
    public void testSubscription() throws Exception {
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        assertEquals(Collections.singleton(topicUriStr1), pscConsumer.subscription());

        // invalid topic uri
        String invalidTopicUriStr = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":backend:env:cloud_region::cluster:topic1";
        Exception e = assertThrows(ConsumerException.class,
                () -> pscConsumer.subscribe(Collections.singleton(invalidTopicUriStr))
        );
        assertEquals(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND("backend"), e.getMessage());

        pscConsumer.unsubscribe();
        pscConsumer.close();
    }

    /**
     * Verifies that consecutive topic subscriptions of a PSC consumer work as expected.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleSubscribeCalls() throws Exception {
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        assertEquals(Collections.singleton(topicUriStr1), pscConsumer.subscription());

        pscConsumer.subscribe(Collections.singleton(topicUriStr2));
        assertEquals(Collections.singleton(topicUriStr2), pscConsumer.subscription());

        pscConsumer.unsubscribe();

        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        assertEquals(Collections.singleton(topicUriStr1), pscConsumer.subscription());

        pscConsumer.unsubscribe();

        pscConsumer.subscribe(Collections.singleton(topicUriStr2));
        assertEquals(Collections.singleton(topicUriStr2), pscConsumer.subscription());

        pscConsumer.unsubscribe();
        pscConsumer.close();
    }

    /**
     * Verifies that consecutive partition assignments of a PSC consumer work as expected.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleAssignCalls() throws Exception {
        int targetPartition1 = 0;
        TopicUriPartition topicUriPartition1 = new TopicUriPartition(topicUriStr1, targetPartition1);
        int targetPartition2 = 0;
        TopicUriPartition topicUriPartition2 = new TopicUriPartition(topicUriStr1, targetPartition2);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        pscConsumer.assign(Collections.singleton(topicUriPartition1));
        assertEquals(Collections.singleton(topicUriPartition1), pscConsumer.assignment());

        pscConsumer.assign(Collections.singleton(topicUriPartition2));
        assertEquals(Collections.singleton(topicUriPartition2), pscConsumer.assignment());

        pscConsumer.unassign();

        pscConsumer.assign(Collections.singleton(topicUriPartition1));
        assertEquals(Collections.singleton(topicUriPartition1), pscConsumer.assignment());

        pscConsumer.unassign();

        pscConsumer.assign(Collections.singleton(topicUriPartition2));
        assertEquals(Collections.singleton(topicUriPartition2), pscConsumer.assignment());

        pscConsumer.unassign();
        pscConsumer.close();
    }
}
