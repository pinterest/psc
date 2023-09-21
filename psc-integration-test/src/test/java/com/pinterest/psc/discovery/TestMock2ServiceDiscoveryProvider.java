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
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class TestMock2ServiceDiscoveryProvider {
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource1 = new SharedKafkaTestResource().withBrokers(1);
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource2 = new SharedKafkaTestResource().withBrokers(1);

    private static final PscConfiguration pscConfiguration = new PscConfiguration();
    private static final String topic1 = "topic1";
    private static final int partitions1 = 3;
    private static final String topicUriPrefix1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region1::cluster1:";
    private static final String topicUriAsString1 = topicUriPrefix1 + topic1;
    private static final String topic2 = "topic2";
    private static final int partitions2 = 6;
    private static final String topicUriPrefix2 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region2::cluster2:";
    private static final String topicUriAsString2 = topicUriPrefix2 + topic2;

    @BeforeEach
    public void setup() throws IOException, InterruptedException {
        pscConfiguration.clear();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-client");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource1, topic1, partitions1);
        PscTestUtils.createTopicAndVerify(sharedKafkaTestResource2, topic2, partitions2);
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
        PscTestUtils.deleteTopicAndVerify(sharedKafkaTestResource2, topic2);
        Thread.sleep(1000);
    }

    @Deprecated
    @Test
    public void testPscConsumerWithNoOpDiscoveryProviderDeprecated() throws ConsumerException, ConfigurationException, WakeupException {
        Configuration configuration = new PropertiesConfiguration();
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-client");
        configuration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        configuration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        configuration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        configuration.setProperty(
                "psc.discovery.topic.uri.prefixes",
                String.join(",", new String[]{topicUriPrefix1, topicUriPrefix2})
        );
        configuration.setProperty(
                "psc.discovery.connection.urls",
                String.join(",", new String[]{
                        sharedKafkaTestResource1.getKafkaBrokers().iterator().next().getConnectString(),
                        sharedKafkaTestResource2.getKafkaBrokers().iterator().next().getConnectString()
                })
        );
        configuration.setProperty(
                "psc.discovery.security.protocols",
                String.join(",", new String[]{"plaintext", "plaintext"})
        );

        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(configuration);

        Set<TopicUriPartition> topicUriPartitions1 = pscConsumer.getPartitions(topicUriAsString1);
        assertEquals(partitions1, topicUriPartitions1.size());

        Set<TopicUriPartition> topicUriPartitions2 = pscConsumer.getPartitions(topicUriAsString2);
        assertEquals(partitions2, topicUriPartitions2.size());

        pscConsumer.close();
    }

    @Test
    public void testPscConsumerWithNoOpDiscoveryProvider() throws ConsumerException, ConfigurationException, WakeupException {
        pscConfiguration.setProperty(
                "psc.discovery.topic.uri.prefixes",
                String.join(",", new String[]{topicUriPrefix1, topicUriPrefix2})
        );
        pscConfiguration.setProperty(
                "psc.discovery.connection.urls",
                String.join(",", new String[]{
                        sharedKafkaTestResource1.getKafkaBrokers().iterator().next().getConnectString(),
                        sharedKafkaTestResource2.getKafkaBrokers().iterator().next().getConnectString()
                })
        );
        pscConfiguration.setProperty(
                "psc.discovery.security.protocols",
                String.join(",", new String[]{"plaintext", "plaintext"})
        );

        PscConsumer<byte[], byte[]> pscConsumer = new PscConsumer<>(pscConfiguration);

        Set<TopicUriPartition> topicUriPartitions1 = pscConsumer.getPartitions(topicUriAsString1);
        assertEquals(partitions1, topicUriPartitions1.size());

        Set<TopicUriPartition> topicUriPartitions2 = pscConsumer.getPartitions(topicUriAsString2);
        assertEquals(partitions2, topicUriPartitions2.size());

        pscConsumer.close();
    }
}