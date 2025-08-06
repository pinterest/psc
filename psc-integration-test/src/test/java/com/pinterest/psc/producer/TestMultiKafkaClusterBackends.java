package com.pinterest.psc.producer;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.integration.KafkaCluster;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestMultiKafkaClusterBackends {
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource1 = new SharedKafkaTestResource()
            .withBrokers(1).registerListener(new PlainListener().onPorts(9092));
    // TODO: junit5 3.2.3 does not support multi-cluster setup

//    @RegisterExtension
//    public static final SharedKafkaTestResource sharedKafkaTestResource2 = new SharedKafkaTestResource()
//            .withBrokers(1).registerListener(new PlainListener().onPorts(9093));

    private static final PscConfiguration producerConfiguration = new PscConfiguration();
    private static String baseProducerId;
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
        baseProducerId = this.getClass().getSimpleName() + "-psc-producer-client";
        producerConfiguration.clear();
        producerConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, baseProducerId + "-" + UUID.randomUUID());
        producerConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        kafkaCluster1 = new KafkaCluster("plaintext", "region", "cluster", 9092);
        topicUriStr1 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster1.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster1.getRegion(), kafkaCluster1.getCluster(), topic1);

        kafkaCluster2 = new KafkaCluster("plaintext", "region2", "cluster2", 9092);
        topicUriStr2 = String.format("%s:%s%s:kafka:env:cloud_%s::%s:%s",
                kafkaCluster2.getTransport(), TopicUri.SEPARATOR, TopicUri.STANDARD, kafkaCluster2.getRegion(), kafkaCluster2.getCluster(), topic1);

        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource1, topic1, partitions1);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource1, topic2, partitions2);
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
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource1, topic2);
        Thread.sleep(1000);
    }

    /**
     * Verifies that backend producers each have their own transactional states that could be different at times.
     *
     * @throws ConfigurationException
     * @throws ProducerException
     */
   @Test
    public void testTransactionalProducersStates() throws ConfigurationException, ProducerException {
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        producerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, "test-transactional-id");

        PscProducer<Integer, Integer> pscProducer = new PscProducer<>(producerConfiguration);
        pscProducer.initTransactions(topicUriStr1);
        assertEquals(PscProducer.TransactionalState.INIT_AND_BEGUN, pscProducer.getTransactionalState());

        PscBackendProducer<Integer, Integer> backendProducer1 = pscProducer.getBackendProducer(topicUriStr1);
        assertEquals(PscProducer.TransactionalState.BEGUN, pscProducer.getBackendProducerState(backendProducer1));

        Exception e = assertThrows(ProducerException.class, () -> pscProducer.beginTransaction());
        assertEquals("Invalid transaction state: consecutive calls to beginTransaction().", e.getMessage());

        PscProducerMessage<Integer, Integer> producerMessage = new PscProducerMessage<>(topicUriStr2, 0);
        pscProducer.send(producerMessage);

        assertEquals(2, pscProducer.getBackendProducers().size());

        PscBackendProducer<Integer, Integer> backendProducer2 = pscProducer.getBackendProducer(topicUriStr2);
        assertNotEquals(backendProducer1, backendProducer2);

        assertEquals(PscProducer.TransactionalState.READY, pscProducer.getBackendProducerState(backendProducer1));
        assertEquals(PscProducer.TransactionalState.IN_TRANSACTION, pscProducer.getBackendProducerState(backendProducer2));

        pscProducer.close();
    }
}
