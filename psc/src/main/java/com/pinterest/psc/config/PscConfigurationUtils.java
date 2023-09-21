package com.pinterest.psc.config;

import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.configuration2.ConfigurationUtils;

import java.util.Properties;

public class PscConfigurationUtils {

    public static void copy(Configuration configuration, PscConfiguration dstConfiguration) {
        ConfigurationUtils.copy(configuration, dstConfiguration);
    }

    public static PscConfigurationInternal propertiesToPscConfigurationInternal(Properties props, String clientType) {
        PscConfiguration pscConfiguration = new PscConfiguration();
        props.keySet().stream().map(Object::toString).filter(key -> key.startsWith("psc.")).forEach(key -> pscConfiguration.setProperty(key, props.get(key)));
        try {
            return new PscConfigurationInternal(pscConfiguration, clientType, true, false);
        } catch (ConfigurationException exception) {
            throw new IllegalArgumentException(exception);
        }
    }

    public static Properties pscConfigurationInternalToProperties(PscConfigurationInternal pscConfigurationInternal) {
        return ConfigurationConverter.getProperties(pscConfigurationInternal.getConfiguration());
    }

}
