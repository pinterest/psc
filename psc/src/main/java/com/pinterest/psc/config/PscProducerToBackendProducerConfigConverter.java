package com.pinterest.psc.config;

import com.pinterest.psc.common.TopicUri;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public abstract class PscProducerToBackendProducerConfigConverter extends PscToBackendConfigConverter {

    public Properties convert(PscConfigurationInternal pscConfigurationInternal, TopicUri topicUri) {
        Properties properties = new Properties();
        PscConfiguration pscProducerConfiguration = pscConfigurationInternal.getProducerConfiguration();

        // first build backend configs for properties in the above map
        PSC_TO_BACKEND_CONFIG_CONVERTER_MAP.forEach((pscConfig, backendConfig) -> {
            if (pscProducerConfiguration.containsKey(pscConfig))
                properties.setProperty(backendConfig, pscProducerConfiguration.getString(pscConfig));
        });

        // next move all other configs over (pass through) only if PSC did not provide a value for them
        pscProducerConfiguration.getKeys().forEachRemaining(key -> {
            if (!PSC_TO_BACKEND_CONFIG_CONVERTER_MAP.containsKey(key) && !properties.containsKey(key))
                properties.setProperty(key, pscProducerConfiguration.getString(key));
        });

        // distinguish different producer objects created on the same psc client instance.
        // this helps avoid issues such as metric tag clashes in the backend metrics processing.
        // example: javax.management.InstanceAlreadyExistsException: kafka.producer:type=app-info,id=test-producer
        // when a psc producer launches multiple kafka producers
        String configuredClientId = properties.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG,
                configuredClientId + String.format("-%s-%s", topicUri.getBackend(), topicUri.getCluster())
        );

        return properties;
    }
}
