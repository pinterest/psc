package com.pinterest.psc.discovery;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.utils.PscTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class TestMockServiceDiscoveryProvider {
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource1 = new SharedKafkaTestResource().withBrokers(1);

    private static final PscConfiguration pscConfiguration = new PscConfiguration();
    private static final String topic1 = "topic1";
    private static final int partitions1 = 3;
    private static final String topicUriPrefix1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region1::cluster1:";
    private static final String topicUriAsString1 = topicUriPrefix1 + topic1;

    @BeforeEach
    public void setup() throws InterruptedException {
        pscConfiguration.clear();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-client");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource1, topic1, partitions1);
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
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource1, topic1);
        Thread.sleep(1000);
    }

    @Test
    public void testPscConsumerWithNoOpDiscoveryProvider() throws ConsumerException, ConfigurationException, WakeupException {
        pscConfiguration.setProperty(
                "psc.discovery.connection.url",
                sharedKafkaTestResource1.getKafkaBrokers().iterator().next().getConnectString()
        );
        pscConfiguration.setProperty("psc.discovery.security.protocol", "plaintext");
        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(pscConfiguration);
        Set<TopicUriPartition> topicUriPartitions = pscConsumer.getPartitions(topicUriAsString1);
        assertEquals(partitions1, topicUriPartitions.size());
        pscConsumer.close();
    }
}