package com.pinterest.psc.consumer;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;

import java.util.Collection;
import java.util.Map;

public class PscConsumerUtils {
    @SuppressWarnings("unchecked")
    public static Map<String, PscConfiguration> getBackendConsumerConfiguration(PscConsumer pscConsumer) {
        return pscConsumer.getBackendConsumerConfigurationPerTopicUri();
    }

    public static Collection<PscBackendConsumer> getBackendConsumersOf(PscConsumer pscConsumer) {
        return pscConsumer.getBackendConsumers();
    }

    public static void resetBackendConsumer(PscConsumer pscConsumer, PscBackendConsumer pscBackendConsumer)
            throws ConfigurationException, ConsumerException {
        pscConsumer.reset(pscBackendConsumer);
    }

    public static PscMetricRegistryManager getMetricRegistryManager(PscConsumer pscConsumer) {
        return pscConsumer.getPscMetricRegistryManager();
    }

    public static PscConfigurationInternal getPscConfigurationInternal(PscConsumer pscConsumer) {
        return pscConsumer.getPscConfiguration();
    }
}
