package com.pinterest.psc.config;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PscConsumerToKafkaConsumerConfigConverter extends PscConsumerToBackendConsumerConfigConverter {
    @Override
    protected Map<String, String> getConfigConverterMap() {
        return new HashMap<String, String>() {
            private static final long serialVersionUID = 1L;

            {
                put(PscConfiguration.ASSIGNMENT_STRATEGY_CLASS, ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
                put(PscConfiguration.BUFFER_RECEIVE_BYTES, ConsumerConfig.RECEIVE_BUFFER_CONFIG);
                put(PscConfiguration.BUFFER_SEND_BYTES, ConsumerConfig.SEND_BUFFER_CONFIG);
                put(PscConfiguration.COMMIT_AUTO_ENABLED, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
                put(PscConfiguration.METADATA_AGE_MAX_MS, ConsumerConfig.METADATA_MAX_AGE_CONFIG);
                put(PscConfiguration.OFFSET_AUTO_RESET, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
                put(PscConfiguration.PARTITION_FETCH_MAX_BYTES, ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
                put(PscConfiguration.POLL_INTERVAL_MAX_MS, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
                put(PscConfiguration.POLL_MESSAGES_MAX, ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
            }
        };
    }

    @Override
    public Properties convert(PscConfigurationInternal pscConfigurationInternal, TopicUri topicUri) {
        Properties properties = super.convert(pscConfigurationInternal, topicUri);
        if (topicUri.getProtocol().equals(KafkaTopicUri.SECURE_PROTOCOL)) {
            properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        } else if (topicUri.getProtocol().equals(KafkaTopicUri.PLAINTEXT_PROTOCOL)) {
            properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);
        }
        return properties;
    }
}
