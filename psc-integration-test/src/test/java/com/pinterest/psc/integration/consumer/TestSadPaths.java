package com.pinterest.psc.integration.consumer;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TestSadPaths {
    private static final PscLogger logger = PscLogger.getLogger(TestSadPaths.class);
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokers(1);

    private static final PscConfiguration pscConfiguration = new PscConfiguration();

    @BeforeEach
    public void setup() {
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-client");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
    }

    /**
     * Verifies that service discovery triggers and processes the discovery fallback file when higher priority providers
     * do not apply.
     *
     * @throws IOException
     * @throws ConsumerException
     * @throws ConfigurationException
     */
    @Test
    public void testServiceDiscoveryFallback() throws IOException, ConsumerException, ConfigurationException {
        String clusterPrefix = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region12::cluster13:";
        String topic = "topic14";
        File tempFile = File.createTempFile("discovery.fallback", ".json", new File("/tmp"));
        tempFile.deleteOnExit();
        String discoveryContent =
                String.format("{\n  \"%s\": {\n", clusterPrefix) +
                        String.format("    \"connect\": \"%s\",\n", sharedKafkaTestResource.getKafkaConnectString()) +
                        "    \"securityProtocol\": \"PLAINTEXT\"\n  }\n}";
        Files.write(tempFile.toPath(), discoveryContent.getBytes());
        pscConfiguration.setProperty(PscConfiguration.PSC_DISCOVERY_FALLBACK_FILE, tempFile.getAbsolutePath());
        PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration);
        String topicUriString = clusterPrefix + topic;
        pscConsumer.subscribe(Collections.singleton(topicUriString));
        assertEquals(Collections.singleton(topicUriString), pscConsumer.subscription());
        pscConsumer.close();
    }
}
