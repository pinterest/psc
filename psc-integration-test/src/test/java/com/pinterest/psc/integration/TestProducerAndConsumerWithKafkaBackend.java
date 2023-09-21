package com.pinterest.psc.integration;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscMessage;
import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.IntegerDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestProducerAndConsumerWithKafkaBackend {
    private static final PscLogger logger = PscLogger.getLogger(TestProducerAndConsumerWithKafkaBackend.class);
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource().withBrokers(1);

    private static final int TEST_TIMEOUT_SECONDS = 60;
    private static final PscConfiguration producerConfiguration = new PscConfiguration();
    private static final PscConfiguration consumerConfiguration = new PscConfiguration();
    private static final PscConfiguration discoveryConfiguration = new PscConfiguration();
    private static String baseProducerClientId;
    private static String baseConsumerClientId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 12;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 1;
    private String topicUriPrefix1, topicUriStr1, topicUriPrefix2, topicUriStr2;
    private AdminClient adminClient;

    /**
     * Initializes a Kafka cluster that is commonly used by all tests, and creates a single topic on it.
     *
     * @throws InterruptedException
     */
    @BeforeEach
    public void setup() throws InterruptedException {
        topicUriPrefix1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:";
        topicUriStr1 = topicUriPrefix1 + topic1;
        topicUriPrefix2 = topicUriPrefix1;
        topicUriStr2 = topicUriPrefix2 + topic2;

        discoveryConfiguration.clear();
        discoveryConfiguration.setProperty("psc.discovery.topic.uri.prefixes", topicUriPrefix1);
        discoveryConfiguration.setProperty("psc.discovery.connection.urls", sharedKafkaTestResource.getKafkaConnectString());
        discoveryConfiguration.setProperty("psc.discovery.security.protocols", "plaintext");

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

        discoveryConfiguration.getKeys().forEachRemaining(key -> {
            producerConfiguration.setProperty(key, discoveryConfiguration.getString(key));
            consumerConfiguration.setProperty(key, discoveryConfiguration.getString(key));
        });

        adminClient = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient();

        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic1, partitions1);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic2, partitions2);
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
        adminClient.close();
        Thread.sleep(1000);
    }

    /**
     * Verifies that timestamp header injection and retrieval works as expected for PSC producer and PSC consumer.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws ConsumerException
     */
    @Deprecated
    @Timeout(TEST_TIMEOUT_SECONDS)
    public void testHeaderTimestampDeprecated() throws ProducerException, ConfigurationException, InterruptedException, ConsumerException {
        Configuration producerConfiguration = new PropertiesConfiguration();
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, baseProducerClientId + "-" + UUID.randomUUID());
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");

        Configuration consumerConfiguration = new PropertiesConfiguration();
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseConsumerClientId + "-" + UUID.randomUUID());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");

        Configuration discoveryConfiguration = new PropertiesConfiguration();
        discoveryConfiguration.setProperty("psc.discovery.topic.uri.prefixes", topicUriPrefix1);
        discoveryConfiguration.setProperty("psc.discovery.connection.urls", sharedKafkaTestResource.getKafkaConnectString());
        discoveryConfiguration.setProperty("psc.discovery.security.protocols", "plaintext");
        discoveryConfiguration.getKeys().forEachRemaining(key -> {
            producerConfiguration.setProperty(key, discoveryConfiguration.getString(key));
            consumerConfiguration.setProperty(key, discoveryConfiguration.getString(key));
        });

        int messageCount = 100;
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);
        PscProducerMessage<String, String> pscProducerMessage;
        List<Future<MessageId>> sendResponseFutures = new ArrayList<>();
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<>(topicUriStr1, "" + i, "" + i, System.currentTimeMillis());
            sendResponseFutures.add(pscProducer.send(pscProducerMessage));
        }
        pscProducer.close();

        while (!sendResponseFutures.isEmpty()) {
            sendResponseFutures.removeIf(Future::isDone);
            Thread.sleep(100);
        }

        long MIN_LAG = 1000;
        Thread.sleep(MIN_LAG);
        long consumptionTimestamp = System.currentTimeMillis();

        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group-" + UUID.randomUUID());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(consumerConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        int count = 0;
        while (true) {
            PscConsumerPollMessageIterator<String, String> messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                PscConsumerMessage<String, String> message = messageIterator.next();
                ++count;
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.HEADER_TIMESTAMP_NOT_FOUND));
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.HEADER_TIMESTAMP_CORRUPTED));
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.HEADER_TIMESTAMP_RETRIEVAL_FAILED));
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.KEY_SIZE_MISMATCH_WITH_KEY_SIZE_HEADER));
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.VALUE_SIZE_MISMATCH_WITH_VALUE_SIZE_HEADER));
                assertTrue(message.getHeaders().containsKey(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP));
                long produceTimestamp = PscCommon.byteArrayToLong(message.getHeader(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP));
                assertTrue(consumptionTimestamp - produceTimestamp >= MIN_LAG);
            }

            if (count >= messageCount)
                break;
        }
        pscConsumer.close();

        assertEquals(messageCount, count);
    }

    /**
     * Verifies that timestamp header injection and retrieval works as expected for PSC producer and PSC consumer.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testHeaderTimestamp() throws ProducerException, ConfigurationException, InterruptedException, ConsumerException {
        int messageCount = 100;
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);
        PscProducerMessage<String, String> pscProducerMessage;
        List<Future<MessageId>> sendResponseFutures = new ArrayList<>();
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<>(topicUriStr1, "" + i, "" + i, System.currentTimeMillis());
            sendResponseFutures.add(pscProducer.send(pscProducerMessage));
        }
        pscProducer.close();

        while (!sendResponseFutures.isEmpty()) {
            sendResponseFutures.removeIf(Future::isDone);
            Thread.sleep(100);
        }

        long MIN_LAG = 1000;
        Thread.sleep(MIN_LAG);
        long consumptionTimestamp = System.currentTimeMillis();

        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group-" + UUID.randomUUID());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(consumerConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        int count = 0;
        while (true) {
            PscConsumerPollMessageIterator<String, String> messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                PscConsumerMessage<String, String> message = messageIterator.next();
                ++count;
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.HEADER_TIMESTAMP_NOT_FOUND));
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.HEADER_TIMESTAMP_CORRUPTED));
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.HEADER_TIMESTAMP_RETRIEVAL_FAILED));
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.KEY_SIZE_MISMATCH_WITH_KEY_SIZE_HEADER));
                assertFalse(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.VALUE_SIZE_MISMATCH_WITH_VALUE_SIZE_HEADER));
                assertTrue(message.getHeaders().containsKey(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP));
                long produceTimestamp = PscCommon.byteArrayToLong(message.getHeader(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP));
                assertTrue(consumptionTimestamp - produceTimestamp >= MIN_LAG);
            }

            if (count >= messageCount)
                break;
        }
        pscConsumer.close();

        assertEquals(messageCount, count);
    }

    /**
     * Verifies that tag injection works as expected when serdes don't match.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testSerdeMismatch() throws ProducerException, ConfigurationException, InterruptedException, ConsumerException {
        int messageCount = 1;
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);
        PscProducerMessage<String, String> pscProducerMessage;
        List<Future<MessageId>> sendResponseFutures = new ArrayList<>();
        for (int i = 0; i < messageCount; ++i) {
            pscProducerMessage = new PscProducerMessage<>(topicUriStr1, "" + i, "" + i, System.currentTimeMillis());
            sendResponseFutures.add(pscProducer.send(pscProducerMessage));
        }
        pscProducer.close();

        while (!sendResponseFutures.isEmpty()) {
            sendResponseFutures.removeIf(Future::isDone);
            Thread.sleep(100);
        }

        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, IntegerDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, IntegerDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group-" + UUID.randomUUID());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(consumerConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr1));
        int count = 0;
        while (true) {
            PscConsumerPollMessageIterator<String, String> messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                PscConsumerMessage<String, String> message = messageIterator.next();
                ++count;
                assertTrue(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.KEY_DESERIALIZATION_FAILED));
                assertTrue(message.getTags().contains(PscConsumerMessage.DefaultPscConsumerMessageTags.VALUE_DESERIALIZATION_FAILED));
            }

            if (count >= messageCount)
                break;
        }
        pscConsumer.close();

        assertEquals(messageCount, count);
    }

    /**
     * Verifies that serialized message key/value sizes are set properly on both producer and consumer sides.
     *
     * @throws ConfigurationException
     * @throws ProducerException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testKeyValueSize() throws ConfigurationException, ProducerException, ExecutionException, InterruptedException, ConsumerException {
        String key1 = "key1";
        String value1 = "value1";

        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);
        PscProducerMessage<String, String> pscProducerMessage1 = new PscProducerMessage<>(topicUriStr2, value1);
        MessageId messageId1 = pscProducer.send(pscProducerMessage1).get();
        assertNotNull(messageId1);
        assertEquals(-1, messageId1.getSerializedKeySizeBytes());
        assertEquals(PscCommon.stringToByteArray(value1).length, messageId1.getSerializedValueSizeBytes());

        PscProducerMessage<String, String> pscProducerMessage2 = new PscProducerMessage<>(topicUriStr2, key1, value1);
        MessageId messageId2 = pscProducer.send(pscProducerMessage2).get();
        assertNotNull(messageId2);
        assertEquals(PscCommon.stringToByteArray(key1).length, messageId2.getSerializedKeySizeBytes());
        assertEquals(PscCommon.stringToByteArray(value1).length, messageId2.getSerializedValueSizeBytes());
        pscProducer.close();

        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group-" + UUID.randomUUID());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(consumerConfiguration);
        pscConsumer.subscribe(Collections.singleton(topicUriStr2));
        int count = 0;
        while (true) {
            PscConsumerPollMessageIterator<String, String> messageIterator = pscConsumer.poll();
            while (messageIterator.hasNext()) {
                switch (count) {
                    case 0:
                        MessageId messageId11 = messageIterator.next().getMessageId();
                        assertNotNull(messageId11);
                        assertEquals(messageId1.getSerializedKeySizeBytes(), messageId11.getSerializedKeySizeBytes());
                        assertEquals(messageId1.getSerializedValueSizeBytes(), messageId11.getSerializedValueSizeBytes());
                        ++count;
                        break;
                    case 1:
                        MessageId messageId22 = messageIterator.next().getMessageId();
                        assertNotNull(messageId22);
                        assertEquals(messageId2.getSerializedKeySizeBytes(), messageId22.getSerializedKeySizeBytes());
                        assertEquals(messageId2.getSerializedValueSizeBytes(), messageId22.getSerializedValueSizeBytes());
                        ++count;
                        break;
                    default:
                        fail();
                }
            }

            if (count > 1)
                break;
        }

        pscConsumer.close();
    }
}
