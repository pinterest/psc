package com.pinterest.psc.consumer;

import com.pinterest.psc.consumer.creation.PscConsumerCreatorManager;
import com.pinterest.psc.consumer.listener.MessageListener;
import com.pinterest.psc.interceptor.Interceptors;
import com.pinterest.psc.metrics.PscMetricRegistryManager;

public class PscConsumerUtils {
    public static MessageListener getMessageListener(PscConsumer pscConsumer) {
        return pscConsumer.getListener();
    }

    public static Interceptors getInterceptors(PscConsumer pscConsumer) {
        return pscConsumer.getInterceptors();
    }

    public static void setCreatorManager(PscConsumer pscConsumer, PscConsumerCreatorManager creatorManager) {
        pscConsumer.setCreatorManager(creatorManager);
    }

    public static void setPscMetricRegistryManager(PscConsumer pscConsumer, PscMetricRegistryManager pscMetricRegistryManager) {
        pscConsumer.setPscMetricRegistryManager(pscMetricRegistryManager);
    }
}
