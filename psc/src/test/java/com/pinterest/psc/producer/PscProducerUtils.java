package com.pinterest.psc.producer;

import com.pinterest.psc.interceptor.Interceptors;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.producer.creation.PscProducerCreatorManager;

public class PscProducerUtils {

    public static void setCreatorManager(PscProducer pscProducer, PscProducerCreatorManager creatorManager) {
        pscProducer.setCreatorManager(creatorManager);
    }

    public static void setPscMetricRegistryManager(PscProducer pscProducer, PscMetricRegistryManager pscMetricRegistryManager) {
        pscProducer.setPscMetricRegistryManager(pscMetricRegistryManager);
    }

    public static Interceptors getInterceptors(PscProducer pscProducer) {
        return pscProducer.getInterceptors();
    }
}
