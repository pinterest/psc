package com.pinterest.flink.connector.psc.sink.testutils;

import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import java.util.Properties;

public class PscTestUtils {

    public static TopicUri getClusterUri() {
        try {
            return new KafkaTopicUri(BaseTopicUri.validate(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX));
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void injectDiscoveryConfigs(Properties properties, String kafkaContainerbootstrapServers, String clusterUriString) {
        String withoutProtocol = kafkaContainerbootstrapServers.split("://")[1];
        String[] components = withoutProtocol.split(",");
        String localhostAndPort = null;
        for (String component : components) {
            if (component.contains("localhost")) {
                localhostAndPort = component;
                break;
            }
        }
        if (localhostAndPort == null) {
            throw new RuntimeException("Cannot find localhost in bootstrap servers");
        }
        properties.setProperty("psc.discovery.topic.uri.prefixes", clusterUriString);
        properties.setProperty("psc.discovery.connection.urls", localhostAndPort);
        properties.setProperty("psc.discovery.security.protocols", "plaintext");
    }
}
