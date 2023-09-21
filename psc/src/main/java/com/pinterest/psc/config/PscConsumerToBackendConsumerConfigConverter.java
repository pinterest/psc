package com.pinterest.psc.config;

import com.pinterest.psc.common.TopicUri;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public abstract class PscConsumerToBackendConsumerConfigConverter extends PscToBackendConfigConverter {

    public Properties convert(PscConfigurationInternal pscConfigurationInternal, TopicUri topicUri) {
        Properties properties = new Properties();
        PscConfiguration pscConsumerConfiguration = pscConfigurationInternal.getConsumerConfiguration();

        // first build backend configs for properties in the PSC-to-backend config map that have been set
        PSC_TO_BACKEND_CONFIG_CONVERTER_MAP.forEach((pscConfig, backendConfig) -> {
            if (pscConsumerConfiguration.containsKey(pscConfig))
                properties.setProperty(backendConfig, pscConsumerConfiguration.getString(pscConfig));
        });

        // next move all other configs over (pass through) only if
        // 1. they're not psc configs (in map keys), and
        // 2. they're not psc-mapped configs (in map values), and
        // 3. they're not set
        pscConsumerConfiguration.getKeys().forEachRemaining(key -> {
            if (
                    !PSC_TO_BACKEND_CONFIG_CONVERTER_MAP.containsKey(key) &&
                    !PSC_TO_BACKEND_CONFIG_CONVERTER_MAP.containsValue(key) &&
                    !properties.containsKey(key)
            )
                properties.setProperty(key, pscConsumerConfiguration.getString(key));
        });

        // distinguish different consumer objects created on the same psc client instance.
        // this helps avoid issues such as metric tag clashes in the backend metrics processing.
        // example: javax.management.InstanceAlreadyExistsException: kafka.consumer:type=app-info,id=test-consumer
        // when a psc consumer launches multiple kafka consumers
        String configuredClientId = properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,
                configuredClientId + String.format("-%s-%s", topicUri.getBackend(), topicUri.getCluster())
        );

        return properties;
    }
}
