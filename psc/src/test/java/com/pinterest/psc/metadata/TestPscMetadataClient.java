package com.pinterest.psc.metadata;

import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.discovery.DiscoveryUtil;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.kafka.PscKafkaMetadataClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPscMetadataClient {

    protected static final String testKafkaTopic1 = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:topic1";

    @Test
    void testGetBackendMetadataClient() throws ConfigurationException, TopicUriSyntaxException, IOException {
        String fallbackDiscoveryFilename = DiscoveryUtil.createTempFallbackFile();
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.addProperty(PscConfiguration.PSC_DISCOVERY_FALLBACK_FILE, fallbackDiscoveryFilename);

        PscMetadataClient pscMetadataClient = new PscMetadataClient(pscConfiguration);
        PscBackendMetadataClient backendMetadataClient = pscMetadataClient.getBackendMetadataClient(TopicUri.validate(testKafkaTopic1));
        assertEquals(PscKafkaMetadataClient.class, backendMetadataClient.getClass());
        assertEquals("kafkacluster01001:9092,kafkacluster01002:9092", backendMetadataClient.getServiceDiscoveryConfig().getConnect());
    }

    @Test
    void testGetTopicRn() throws IOException, ConfigurationException, TopicUriSyntaxException {
        String fallbackDiscoveryFilename = DiscoveryUtil.createTempFallbackFile();
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.addProperty(PscConfiguration.PSC_DISCOVERY_FALLBACK_FILE, fallbackDiscoveryFilename);

        PscMetadataClient pscMetadataClient = new PscMetadataClient(pscConfiguration);
        PscBackendMetadataClient backendMetadataClient = pscMetadataClient.getBackendMetadataClient(TopicUri.validate(testKafkaTopic1));
        TopicRn topic1Rn = TopicUri.validate(testKafkaTopic1).getTopicRn();
        TopicRn topic2Rn = backendMetadataClient.getTopicRn("topic2");
        assertEquals(topic1Rn.getStandard(), topic2Rn.getStandard());
        assertEquals(topic1Rn.getService(), topic2Rn.getService());
        assertEquals(topic1Rn.getEnvironment(), topic2Rn.getEnvironment());
        assertEquals(topic1Rn.getCloud(), topic2Rn.getCloud());
        assertEquals(topic1Rn.getRegion(), topic2Rn.getRegion());
        assertEquals(topic1Rn.getClassifier(), topic2Rn.getClassifier());
        assertEquals(topic1Rn.getCluster(), topic2Rn.getCluster());
    }
}
