package com.pinterest.psc.exception.handler;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.exception.BackendException;
import com.pinterest.psc.exception.PscException;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricType;
import com.pinterest.psc.logging.PscLogger;

import java.util.Map;
import java.util.Set;

/**
 * Unified error handling for exceptions caught by PSC clients.
 * When an exception is caught by PSC client, the common logic of error handling should go here (e.g. metrics emission)
 */
public class PscErrorHandler {
    private static final PscLogger logger = PscLogger.getLogger(PscErrorHandler.class);
    private static final PscMetricRegistryManager metricRegistryManager = PscMetricRegistryManager.getInstance();

    public static void handle(PscException e, Set<Object> topicUriOrPartitions, boolean emitMetrics, PscConfigurationInternal pscConfigurationInternal) {
        if (topicUriOrPartitions == null)
            handle(e, (Object) null, emitMetrics, pscConfigurationInternal);
        else {
            for (Object object : topicUriOrPartitions) {
                if (object instanceof TopicUri || object instanceof TopicUriPartition)
                    handle(e, object, emitMetrics, pscConfigurationInternal);
            }
        }
    }

    public static void handle(PscException e, Object topicUriOrPartition, boolean emitMetrics, PscConfigurationInternal pscConfigurationInternal) {
        if (topicUriOrPartition == null)
            handle(e, null, PscUtils.NO_PARTITION, emitMetrics, pscConfigurationInternal);
        else if (topicUriOrPartition instanceof TopicUri)
            handle(e, (TopicUri) topicUriOrPartition, PscUtils.NO_PARTITION, emitMetrics, pscConfigurationInternal);
        else if (topicUriOrPartition instanceof TopicUriPartition) {
            TopicUriPartition topicUriPartition = (TopicUriPartition) topicUriOrPartition;
            handle(e, topicUriPartition.getTopicUri(), topicUriPartition.getPartition(), emitMetrics, pscConfigurationInternal);
        }
    }

    public static void handle(PscException e, TopicUri topicUri, int partition, boolean emitMetrics, PscConfigurationInternal pscConfigurationInternal) {
        handle(e, topicUri, partition, e.getMetricName(), PscMetricType.COUNTER, 1L, emitMetrics, pscConfigurationInternal, null);
    }

    public static void handle(PscException e,
                              TopicUri topicUri,
                              int partition,
                              String metricName,
                              PscMetricType metricType,
                              Long metricValueUpdate,
                              boolean emitMetrics,
                              PscConfigurationInternal pscConfigurationInternal,
                              Map<String, Object> additionalHandlerProperties) {
        if (e == null) {
            logger.warn("Exception object is null. Skip handling");
            return;
        }

        if (emitMetrics && metricRegistryManager != null && metricName != null &&
                metricType != null && metricValueUpdate != null) {
            executeMetricsEmission(
                    e,
                    topicUri,
                    partition,
                    metricName,
                    metricType,
                    metricValueUpdate,
                    pscConfigurationInternal
            );
        }

        // exception is now handled
    }

    private static void executeMetricsEmission(PscException e,
                                               TopicUri topicUri,
                                               int partition,
                                               String metricName,
                                               PscMetricType metricType,
                                               long metricValueUpdate,
                                               PscConfigurationInternal pscConfigurationInternal) {

        if (metricRegistryManager == null || metricName == null || metricType == null) {
            return;
        }
        boolean isBackend = e instanceof BackendException;

        switch (metricType) {
            case COUNTER:
                if (isBackend) {
                    metricRegistryManager
                            .incrementBackendCounterMetric(topicUri, partition, metricName, metricValueUpdate, pscConfigurationInternal);
                } else {
                    metricRegistryManager.incrementCounterMetric(topicUri, partition, metricName, metricValueUpdate, pscConfigurationInternal);
                }
                break;
            case HISTOGRAM:
                if (isBackend) {
                    metricRegistryManager
                            .updateBackendHistogramMetric(topicUri, partition, metricName, metricValueUpdate, pscConfigurationInternal);
                } else {
                    metricRegistryManager.updateHistogramMetric(topicUri, partition, metricName, metricValueUpdate, pscConfigurationInternal);
                }
        }
    }

    public enum ActionType {
        NONE, THROW, RETRY_THEN_THROW, RESET_THEN_THROW, RETRY_RESET_THEN_THROW
    }

    public static abstract class Action {
        public final ActionType actionType;
        public Action(ActionType actionType) {
            this.actionType = actionType;
        }
    }

    public static class ConsumerAction extends Action {
        public final Class<? extends ConsumerException> consumerExceptionClass;
        public ConsumerAction(ActionType actionType, Class<? extends ConsumerException> consumerExceptionClass) {
            super(actionType);
            this.consumerExceptionClass = consumerExceptionClass;
        }
    }

    public static class ProducerAction extends Action {
        public final Class<? extends ProducerException> producerExceptionClass;
        public ProducerAction(ActionType actionType, Class<? extends ProducerException> producerExceptionClass) {
            super(actionType);
            this.producerExceptionClass = producerExceptionClass;
        }
    }
}
