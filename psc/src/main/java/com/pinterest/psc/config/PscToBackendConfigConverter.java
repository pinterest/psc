package com.pinterest.psc.config;

import com.pinterest.psc.common.TopicUri;

import java.util.Map;
import java.util.Properties;

public abstract class PscToBackendConfigConverter {
    // a map of PSC Config to backend config; sample entry
    // poll.messages.max -> max.poll.records
    protected final Map<String, String> PSC_TO_BACKEND_CONFIG_CONVERTER_MAP = getConfigConverterMap();

    // interface to extend for each backend converter class
    protected abstract Map<String, String> getConfigConverterMap();

    protected abstract Properties convert(PscConfigurationInternal pscConfigurationInternal, TopicUri topicUri);
}
