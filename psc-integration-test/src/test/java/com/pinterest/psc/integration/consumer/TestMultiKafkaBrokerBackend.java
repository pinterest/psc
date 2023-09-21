package com.pinterest.psc.integration.consumer;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.producer.SerializerException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.integration.producer.PscProducerRunner;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.KafkaBroker;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.vavr.control.Either;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestMultiKafkaBrokerBackend {
    private static final PscLogger logger = PscLogger.getLogger(TestMultiKafkaBrokerBackend.class);
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokers(2)
            .withBrokerProperty("auto.create.topics.enable", "false");

    private static AdminClient adminClient;
    private static final int TEST_TIMEOUT_SECONDS = 60;
    private static final int PSC_CONSUME_TIMEOUT_MS = 7500;
    private static final PscConfiguration consumerConfiguration = new PscConfiguration();
    private static final PscConfiguration producerConfiguration = new PscConfiguration();
    private static String baseProducerId;
    private static String baseConsumerId;
    private static String baseGroupId;
    private static final String topic1 = "topic1";
    private static final int partitions1 = 1;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 24;
    private KafkaCluster kafkaCluster;
    private String topicUriStr1, topicUriStr2;

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
     */
    @BeforeEach
    public void setup() {
        baseConsumerId = this.getClass().getSimpleName() + "-psc-consumer-client";
        baseGroupId = this.getClass().getSimpleName() + "-psc-consumer-group";

        producerConfiguration.clear();
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, baseProducerId + "-" + UUID.randomUUID());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        producerConfiguration.setProperty("psc.discovery.topic.uri.prefixes", "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:");
        producerConfiguration.setProperty("psc.discovery.security.protocols", "plaintext");

        consumerConfiguration.clear();
        consumerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, baseConsumerId + "-" + UUID.randomUUID());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, baseGroupId + "-" + UUID.randomUUID());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        consumerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        consumerConfiguration.setProperty("psc.discovery.topic.uri.prefixes", "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:");
        consumerConfiguration.setProperty("psc.discovery.security.protocols", "plaintext");

        topicUriStr1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region::cluster:" + topic1;
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
        // add a delay to make sure topic is deleted
        Thread.sleep(1000);
    }

    /**
     * Verifies this scenario:
     * 1. Start with 2 brokers (0, 1), one of them (1) shut down
     * 2. Create topic (should go to broker 0 only)
     * 3. Configure consumer to bootstrap only with broker 0
     * 4. Start consumption
     * 5. Start broker 1
     * 6. Move topic to broker 1 only (enable this test when API for this operation is available)
     * 7. Stop broker 0
     * 8. Consumption should continue
     *
     * @throws SerializerException
     * @throws InterruptedException
     */
    @Disabled
    @Timeout(TEST_TIMEOUT_SECONDS)
    @Test
    public void testTopicMovementToNewBroker() throws Exception {
        List<KafkaBroker> brokers = sharedKafkaTestResource.getKafkaBrokers().asList();
        assertEquals(2, brokers.size());
        int id0 = brokers.get(0).getBrokerId();
        int id1 = brokers.get(1).getBrokerId();

        //1
        sharedKafkaTestResource.getKafkaBrokers().getBrokerById(id0).stop();

        //2
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource, topic1, partitions1);

        //3
        consumerConfiguration.setProperty("psc.discovery.connection.urls",
                sharedKafkaTestResource.getKafkaBrokers().getBrokerById(id0).getConnectString());
        producerConfiguration.setProperty("psc.discovery.connection.urls",
                sharedKafkaTestResource.getKafkaBrokers().getBrokerById(id0).getConnectString());
        PscProducerRunner pscProducerRunner = new PscProducerRunner(
                producerConfiguration, Either.left(topicUriStr1), true
        );
        pscProducerRunner.kickoff();

        //4
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(consumerConfiguration);
        PscConsumerRunner<String, String> pscConsumerRunner = new PscConsumerRunner<>(
                consumerConfiguration, Either.left(Collections.singleton(topicUriStr1)), PSC_CONSUME_TIMEOUT_MS
        );
        pscConsumerRunner.kickoff();

        //5
        Thread.sleep(1000);
        sharedKafkaTestResource.getKafkaBrokers().getBrokerById(id1).start();

        //6? (not supported through the APIs in 2.3)
        int producedCount1 = pscProducerRunner.getProducedMessageCount();

        //7
        //Thread.sleep(1000);
        //sharedKafkaTestResource.getKafkaBrokers().getBrokerById(id0).stop();

        //8
        Thread.sleep(5000);
        pscProducerRunner.wakeupProducer();
        //pscProducerRunner.waitForCompletion();
        pscConsumerRunner.wakeupConsumer();
        pscConsumerRunner.waitForCompletion();
        int consumedCount = pscConsumerRunner.getMessages().size();

        assertNull(pscProducerRunner.getException());
        assertNull(pscConsumerRunner.getException());
        //assertEquals(WakeupException.class, pscConsumerRunner.getException().getClass());

        int producedCount2 = pscProducerRunner.getProducedMessageCount();
        assertTrue(producedCount2 > producedCount1);
        assertTrue(consumedCount > producedCount1);
        assertTrue(consumedCount <= producedCount2);

        //cleanup
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource, topic1);
    }
}
