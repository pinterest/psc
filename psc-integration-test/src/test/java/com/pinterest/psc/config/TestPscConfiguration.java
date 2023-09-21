package com.pinterest.psc.config;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestPscConfiguration {
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource().withBrokers(1);

    private static final int TEST_TIMEOUT_SECONDS = 60;
    private static final PscConfiguration producerConfiguration = new PscConfiguration();
    private static final PscConfiguration consumerConfiguration = new PscConfiguration();
    private static final PscConfiguration discoveryConfiguration = new PscConfiguration();
    private static String baseProducerClientId, baseConsumerClientId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 1;
    private static final String configLoggingTopic = "config-logging";
    private static final int configLoggingTopicPartitions = 1;
    private String topicUriPrefix1, topicUriStr1, configLoggingTopicUriPrefix, configLoggingTopicUriStr;
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
        configLoggingTopicUriPrefix = topicUriPrefix1;
        configLoggingTopicUriStr = configLoggingTopicUriPrefix + configLoggingTopic;

        discoveryConfiguration.clear();
        discoveryConfiguration.setProperty("psc.discovery.topic.uri.prefixes", topicUriPrefix1);
        discoveryConfiguration.setProperty("psc.discovery.connection.urls", sharedKafkaTestResource.getKafkaConnectString());
        discoveryConfiguration.setProperty("psc.discovery.security.protocols", "plaintext");

        baseConsumerClientId = this.getClass().getSimpleName() + "-psc-consumer-client";
        consumerConfiguration.clear();
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseConsumerClientId + "-" + UUID.randomUUID());

        baseProducerClientId = this.getClass().getSimpleName() + "-psc-producer-client";
        producerConfiguration.clear();
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, baseProducerClientId + "-" + UUID.randomUUID());

        discoveryConfiguration.getKeys().forEachRemaining(key -> {
            producerConfiguration.setProperty(key, discoveryConfiguration.getString(key));
            consumerConfiguration.setProperty(key, discoveryConfiguration.getString(key));
        });

        adminClient = sharedKafkaTestResource.getKafkaTestUtils().getAdminClient();

        sharedKafkaTestResource.getKafkaTestUtils().createTopic(topic1, partitions1, (short) 1);
        sharedKafkaTestResource.getKafkaTestUtils().createTopic(configLoggingTopic, configLoggingTopicPartitions, (short) 1);
        // add a delay to make sure topic is created
        Thread.sleep(1000);
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
        adminClient.deleteTopics(Arrays.asList(topic1, configLoggingTopic)).all().get();
        // add a delay to make sure topic is deleted
        Thread.sleep(1000);
        adminClient.close();
    }

    /**
     * Verifies that configuration logging works as expected.
     *
     * @throws ProducerException
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws ConsumerException
     */
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testConfigLogging() throws ProducerException, ConfigurationException, InterruptedException, ConsumerException {
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "true");
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_TOPIC_URI, configLoggingTopicUriStr);
        PscProducer<String, String> pscProducer = new PscProducer<>(producerConfiguration);
        pscProducer.close();

        Thread.sleep(5000);

        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group-" + UUID.randomUUID());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(consumerConfiguration);
        pscConsumer.subscribe(Collections.singleton(configLoggingTopicUriStr));
        TopicUriPartition configLoggingTopicUriPartition = new TopicUriPartition(configLoggingTopicUriStr, 0);

        Long endOffset = pscConsumer.endOffsets(Collections.singleton(configLoggingTopicUriPartition))
                .get(configLoggingTopicUriPartition);
        assertNotNull(endOffset);
        assertEquals(1L, endOffset.longValue());
        PscConsumerMessage<String, String> message = null;
        while (message == null) {
            PscConsumerPollMessageIterator<String, String> messageIterator = pscConsumer.poll();
            if (messageIterator.hasNext())
                message = messageIterator.next();
        }
        pscConsumer.close();

        assertTrue(message.getValue().contains("\"psc.client.type\":\"producer\""));
        assertFalse(message.getValue().contains("\"psc.client.type\":\"consumer\""));
    }
}
