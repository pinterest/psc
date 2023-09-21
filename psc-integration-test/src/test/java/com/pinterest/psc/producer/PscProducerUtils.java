package com.pinterest.psc.producer;

import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;

import java.util.Collection;

public class PscProducerUtils {
    public static Collection<PscBackendProducer> getBackendProducersOf(PscProducer pscProducer) {
        return pscProducer.getBackendProducers();
    }

    public static void resetBackendProducer(PscProducer pscProducer, PscBackendProducer pscBackendProducer)
            throws ProducerException, ConfigurationException {
        pscProducer.reset(pscBackendProducer);
    }

    public static PscProducerTransactionalProperties initTransactions(PscProducer pscProducer, String topicUri)
            throws ConfigurationException, ProducerException {
        return pscProducer.initTransactions(topicUri);
    }

    public static PscMetricRegistryManager getMetricRegistryManager(PscProducer pscProducer) {
        return pscProducer.getPscMetricRegistryManager();
    }

    public static PscConfigurationInternal getPscConfigurationInternal(PscProducer pscProducer) {
        return pscProducer.getPscConfiguration();
    }
}
