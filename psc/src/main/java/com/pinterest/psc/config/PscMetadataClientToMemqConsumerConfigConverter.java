package com.pinterest.psc.config;

import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.psc.common.TopicUri;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PscMetadataClientToMemqConsumerConfigConverter extends PscMetadataClientToBackendMetatadataClientConfigCoverter {
    @Override
    protected Map<String, String> getConfigConverterMap() {
        return new HashMap<String, String>() {
            private static final long serialVersionUID = 1L;

            {
                put(PscConfiguration.PSC_METADATA_CLIENT_ID, ConsumerConfigs.CLIENT_ID);
            }
        };
    }

    @Override
    public Properties convert(PscConfigurationInternal pscConfigurationInternal, TopicUri topicUri) {
        return super.convert(pscConfigurationInternal, topicUri);
    }
}
