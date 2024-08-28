package com.pinterest.psc.metadata.client;

import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.discovery.DiscoveryUtil;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.MetadataUtils;
import com.pinterest.psc.metadata.client.kafka.PscKafkaMetadataClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPscMetadataClient {

    protected static final String testKafkaTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:topic1";

    @Test
    void testGetBackendMetadataClient() throws Exception {
        String fallbackDiscoveryFilename = DiscoveryUtil.createTempFallbackFile();
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.addProperty(PscConfiguration.PSC_DISCOVERY_FALLBACK_FILE, fallbackDiscoveryFilename);
        pscConfiguration.addProperty(PscConfiguration.PSC_METADATA_CLIENT_ID, "test-metadata-client");

        PscMetadataClient pscMetadataClient = new PscMetadataClient(pscConfiguration);
        PscBackendMetadataClient backendMetadataClient = pscMetadataClient.getBackendMetadataClient(TopicUri.validate(testKafkaTopic1));
        assertEquals(PscKafkaMetadataClient.class, backendMetadataClient.getClass());
        assertEquals("kafkacluster01001:9092,kafkacluster01002:9092", backendMetadataClient.getDiscoveryConfig().getConnect());
        pscMetadataClient.close();
    }

    @Test
    void testCreateTopicRn() throws TopicUriSyntaxException {
        TopicRn topic1Rn = TopicUri.validate(testKafkaTopic1).getTopicRn();
        TopicRn topic1RnCreated = MetadataUtils.createTopicRn(TopicUri.validate(testKafkaTopic1), "topic1");
        assertTrue(topic1Rn.equals(topic1RnCreated));   // ensure that equality is implemented correctly

        TopicRn topic2Rn = MetadataUtils.createTopicRn(TopicUri.validate(testKafkaTopic1), "topic2");

        // Ensure that the new topic name is the only difference
        assertEquals(topic1Rn.getStandard(), topic2Rn.getStandard());
        assertEquals(topic1Rn.getService(), topic2Rn.getService());
        assertEquals(topic1Rn.getEnvironment(), topic2Rn.getEnvironment());
        assertEquals(topic1Rn.getCloud(), topic2Rn.getCloud());
        assertEquals(topic1Rn.getRegion(), topic2Rn.getRegion());
        assertEquals(topic1Rn.getClassifier(), topic2Rn.getClassifier());
        assertEquals(topic1Rn.getCluster(), topic2Rn.getCluster());
        assertEquals("topic2", topic2Rn.getTopic());
    }
}
