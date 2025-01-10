package com.pinterest.psc.config;

import com.pinterest.psc.common.TopicUri;

import java.util.Properties;

public abstract class PscMetadataClientToBackendMetatadataClientConfigCoverter extends PscToBackendConfigConverter {

    @Override
    protected Properties convert(PscConfigurationInternal pscConfigurationInternal, TopicUri topicUri) {
        Properties properties = new Properties();
        PscConfiguration pscMetadataConfiguration = pscConfigurationInternal.getMetadataClientConfiguration();

        // first build backend configs for properties in the above map
        PSC_TO_BACKEND_CONFIG_CONVERTER_MAP.forEach((pscConfig, backendConfig) -> {
            if (pscMetadataConfiguration.containsKey(pscConfig))
                properties.setProperty(backendConfig, pscMetadataConfiguration.getString(pscConfig));
        });

        // next move all other configs over (pass through) only if PSC did not provide a value for them
        pscMetadataConfiguration.getKeys().forEachRemaining(key -> {
            if (!PSC_TO_BACKEND_CONFIG_CONVERTER_MAP.containsKey(key) && !properties.containsKey(key))
                properties.setProperty(key, pscMetadataConfiguration.getString(key));
        });

        return properties;
    }
}
