package com.pinterest.psc.config;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PscMetadataClientToKafkaAdminClientConfigConverter extends PscMetadataClientToBackendMetatadataClientConfigCoverter {
    @Override
    protected Map<String, String> getConfigConverterMap() {
        return new HashMap<String, String>() {

            private static final long serialVersionUID = 1L;

            {
                put(PscConfiguration.PSC_METADATA_CLIENT_ID, AdminClientConfig.CLIENT_ID_CONFIG);
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
