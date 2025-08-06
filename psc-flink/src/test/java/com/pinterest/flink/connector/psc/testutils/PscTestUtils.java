package com.pinterest.flink.connector.psc.testutils;

import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public class PscTestUtils {

    public static TopicUri getClusterUri() {
        try {
            return new KafkaTopicUri(BaseTopicUri.validate(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX));
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void injectDiscoveryConfigs(Properties properties, String kafkaContainerbootstrapServers, String clusterUriString) {
        String withoutProtocol = kafkaContainerbootstrapServers.split("://")[1];
        String[] components = withoutProtocol.split(",");
        String bootstrapBrokers = null;
        for (String component : components) {
            if (component.contains("localhost")) {
                bootstrapBrokers = component;
                break;
            }
        }
        if (bootstrapBrokers == null) {
            throw new RuntimeException("Cannot find localhost in bootstrap servers");
        }
        putDiscoveryProperties(properties, bootstrapBrokers, clusterUriString);
    }

    public static void putDiscoveryProperties(Properties properties, String bootstrapBrokers, String clusterUriString) {
        int numBootstrapBrokers = bootstrapBrokers.split(",").length;
        properties.setProperty("psc.discovery.topic.uri.prefixes", StringUtils.repeat(clusterUriString, ",", numBootstrapBrokers));
        properties.setProperty("psc.discovery.connection.urls", bootstrapBrokers);
        properties.setProperty("psc.discovery.security.protocols", StringUtils.repeat("plaintext", ",", numBootstrapBrokers));
    }
}
