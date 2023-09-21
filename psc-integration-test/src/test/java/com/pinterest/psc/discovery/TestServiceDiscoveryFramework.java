package com.pinterest.psc.discovery;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestServiceDiscoveryFramework {
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources0 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources1 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources2 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources3 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources4 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources5 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources6 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources7 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources8 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResources9 =
            new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener());

    private final SharedKafkaTestResource[] sharedKafkaTestResources = {
            sharedKafkaTestResources0, sharedKafkaTestResources1, sharedKafkaTestResources2,
            sharedKafkaTestResources3, sharedKafkaTestResources4, sharedKafkaTestResources5,
            sharedKafkaTestResources6, sharedKafkaTestResources7, sharedKafkaTestResources8,
            sharedKafkaTestResources9
    };
    private final String[] topicUriPrefixes = new String[sharedKafkaTestResources.length];
    private final String[] connectionUrls = new String[sharedKafkaTestResources.length];
    private final PscConfiguration pscConfiguration = new PscConfiguration();

    @BeforeEach
    public void setup() {
        pscConfiguration.clear();
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        for (int i = 0; i < sharedKafkaTestResources.length; ++i) {
            topicUriPrefixes[i] = String.format("plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region%d::cluster%d:", i, i);
            connectionUrls[i] = sharedKafkaTestResources[i].getKafkaConnectString();
        }
    }

    @Test
    public void testMultithreadingWithMock2DiscoveryProvider0() throws Exception {
        runMultipleConsumersWithMock2DiscoveryProvider(1, 1, 1);
    }

    @Test
    public void testMultithreadingWithMock2DiscoveryProvider1() throws Exception {
        runMultipleConsumersWithMock2DiscoveryProvider(1, 100, sharedKafkaTestResources.length);
    }

    @Test
    public void testMultithreadingWithMock2DiscoveryProvider2() throws Exception {
        runMultipleConsumersWithMock2DiscoveryProvider(1, 100, 1);
    }

    @Disabled("Flaky test")
    @Test
    public void testMultithreadingWithMock2DiscoveryProvider3() throws Exception {
        runMultipleConsumersWithMock2DiscoveryProvider(100, 100, 1);
    }

    public void runMultipleConsumersWithMock2DiscoveryProvider(
            int numPscConsumers, int numberOfTopics, int maxClusters
    ) throws Exception {
        Map<String, TopicUriInfo> topicUriInfos = generateRandomTopicUriInfos(numberOfTopics, maxClusters);
        Set<String> topicUrisAsString = topicUriInfos.keySet();

        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-client");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty("psc.discovery.topic.uri.prefixes", String.join(",", topicUriPrefixes));
        pscConfiguration.setProperty("psc.discovery.connection.urls", String.join(",", connectionUrls));
        pscConfiguration.setProperty("psc.discovery.security.protocols", String.join(",", Collections.nCopies(sharedKafkaTestResources.length, "plaintext")));

        PscConsumerCreator<byte[], byte[]>[] pscConsumerCreators = new PscConsumerCreator[numPscConsumers];

        for (int i = 0; i < numPscConsumers; ++i)
            pscConsumerCreators[i] = new PscConsumerCreator<>(pscConfiguration, topicUrisAsString);

        for (PscConsumerCreator pscConsumerCreator : pscConsumerCreators)
            pscConsumerCreator.kickoff();

        Thread.sleep(5000);

        for (PscConsumerCreator pscConsumerCreator : pscConsumerCreators)
            pscConsumerCreator.waitForCompletion();

        for (PscConsumerCreator pscConsumerCreator : pscConsumerCreators) {
            Map<String, String> connectionStringByTopicUriString = pscConsumerCreator.getConnectionStringByTopicUriString();
            assertEquals(numberOfTopics, connectionStringByTopicUriString.size());
            for (String topicUriAsString : topicUrisAsString) {
                assertEquals(
                        topicUriInfos.get(topicUriAsString).getConnectionUrl(),
                        connectionStringByTopicUriString.get(topicUriAsString)
                );
            }
        }
    }

    private Map<String, TopicUriInfo> generateRandomTopicUriInfos(int numberOfTopics, int maxClusters) throws Exception {
        Random random = new Random();
        Map<String, TopicUriInfo> topicUriInfos = new HashMap<>();
        for (int i = 0; i < numberOfTopics; ++i) {
            TopicUriInfo topicUriInfo = new TopicUriInfo(random.nextInt(maxClusters));
            topicUriInfos.put(topicUriInfo.getTopicUriAsString(), topicUriInfo);
        }
        return topicUriInfos;
    }

    class TopicUriInfo {
        private final String topicUriPrefix;
        private final String topic;
        private final String connectionUrl;

        public TopicUriInfo(int clusterIndex) throws Exception {
            if (clusterIndex > sharedKafkaTestResources.length) {
                throw new Exception(String.format("Given cluster index (%d) is larger than available count (%d).",
                        clusterIndex, sharedKafkaTestResources.length
                ));
            }

            this.topicUriPrefix = topicUriPrefixes[clusterIndex];
            this.connectionUrl = connectionUrls[clusterIndex];
            this.topic = UUID.randomUUID().toString();
        }

        public String getTopicUriAsString() {
            return topicUriPrefix + topic;
        }

        public String getConnectionUrl() {
            return connectionUrl;
        }
    }
}
