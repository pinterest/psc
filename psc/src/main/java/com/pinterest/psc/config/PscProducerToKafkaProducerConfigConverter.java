package com.pinterest.psc.config;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PscProducerToKafkaProducerConfigConverter extends PscProducerToBackendProducerConfigConverter {
    @Override
    protected Map<String, String> getConfigConverterMap() {
        return new HashMap<String, String>() {
            private static final long serialVersionUID = 1L;

            {
                put(PscConfiguration.BATCH_DURATION_MAX_MS, ProducerConfig.LINGER_MS_CONFIG);
                put(PscConfiguration.BATCH_SIZE_BYTES, ProducerConfig.BATCH_SIZE_CONFIG);
                put(PscConfiguration.BUFFER_MEMORY_BYTES, ProducerConfig.BUFFER_MEMORY_CONFIG);
                put(PscConfiguration.BUFFER_RECEIVE_BYTES, ProducerConfig.RECEIVE_BUFFER_CONFIG);
                put(PscConfiguration.BUFFER_SEND_BYTES, ProducerConfig.SEND_BUFFER_CONFIG);
                put(PscConfiguration.IDEMPOTENCE_ENABLED, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
                put(PscConfiguration.INFLIGHT_REQUESTS_PER_CONNECTION_MAX,
                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
                );
                put(PscConfiguration.METADATA_AGE_MAX_MS, ProducerConfig.METADATA_MAX_AGE_CONFIG);
                put(PscConfiguration.REQUEST_SIZE_MAX_BYTES, ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
                put(PscConfiguration.RETRIES, ProducerConfig.RETRIES_CONFIG);
                /*
                put(PscConfiguration.SSL_KEY_PASSWORD, SslConfigs.SSL_KEY_PASSWORD_CONFIG);
                put(PscConfiguration.SSL_ENABLED_PROTOCOLS, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
                put(PscConfiguration.SSL_KEYSTORE_LOCATION, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
                put(PscConfiguration.SSL_KEYSTORE_PASSWORD, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
                put(PscConfiguration.SSL_KEYSTORE_TYPE, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
                put(PscConfiguration.SSL_TRUSTSTORE_LOCATION, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
                put(PscConfiguration.SSL_TRUSTSTORE_PASSWORD, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
                put(PscConfiguration.SSL_TRUSTSTORE_TYPE, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
                */
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
