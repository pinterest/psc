package com.pinterest.psc.consumer.memq;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.logging.PscLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemqMetricsHandler {

    private static final String MEMQ_DIRECT_MEMORY_USED_METRIC = "netty.direct.memory.used";
    private static final String MEMQ_HEAP_MEMORY_USED_METRIC = "netty.heap.memory.used";

    private static final PscLogger logger = PscLogger.getLogger(MemqMetricsHandler.class);
    private final static Map<String, String> memqConsumerMetricsMap = new ConcurrentHashMap<>();
    private final static Map<String, String> memqConsumerMetricsPrefixMap = new HashMap<>();

    static {
        memqConsumerMetricsPrefixMap.put(AbstractS3StorageHandler.OBJECT_FETCH_ERROR_KEY, "consumer.fetch.error");
        memqConsumerMetricsMap.put("memqConsumer.messagesProcessedCounter", "consumer.processed.messages");
        memqConsumerMetricsMap.put("memqConsumer.bytesProcessedCounter", "consumer.processed.bytes");
        memqConsumerMetricsMap.put("objectFetchLatencyMs", "consumer.fetch.latency.ms");
        memqConsumerMetricsMap.put("iterator.exception", "consumer.iterator.processing.error");
        memqConsumerMetricsMap.put("loading.exception", "consumer.iterator.load.error");
        memqConsumerMetricsMap.put(MEMQ_DIRECT_MEMORY_USED_METRIC, MEMQ_DIRECT_MEMORY_USED_METRIC);
        memqConsumerMetricsMap.put(MEMQ_HEAP_MEMORY_USED_METRIC, MEMQ_HEAP_MEMORY_USED_METRIC);
    }

    public static void handleMemqConsumerMetrics(MetricRegistry memqConsumerMetrics,
                                                 String topicName,
                                                 Map<String, TopicUri> memqTopicToTopicUri,
                                                 PscConfigurationInternal pscConfigurationInternal) {
        if (memqConsumerMetrics == null || memqConsumerMetrics.getMetrics() == null)
            return;

        for (Map.Entry<String, Metric> entry : memqConsumerMetrics.getMetrics().entrySet()) {
            String name = entry.getKey();
            if (memqConsumerMetricsMap.containsKey(name)) {
                Metric metric = entry.getValue();
                if (metric != null) {
                    try {
                        PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                                memqTopicToTopicUri.get(topicName), memqConsumerMetricsMap.get(name),
                                getMetricValue(metric), pscConfigurationInternal);
                    } catch (NumberFormatException nfe) {
                        logger.info("Metric reporting failed", nfe);
                    }
                }
            }
        }
    }

    private static long getMetricValue(Metric metric) {
        if (metric instanceof Counter)
            return ((Counter) metric).getCount();
        if (metric instanceof Meter)
            return ((Meter) metric).getCount();
        if (metric instanceof Histogram)
            return ((Histogram) metric).getSnapshot().getMax();
        if (metric instanceof Timer)
            return ((Timer) metric).getSnapshot().getMax();
        if (metric instanceof Gauge) {
            Object val = ((Gauge<?>) metric).getValue();
            if (val instanceof Long) return (Long) val;
            if (val instanceof Double) return ((Double) val).longValue();
        }

        logger.warn("[Memq] Could not process metric of type {}", metric.getClass().getName());
        return -1;
    }

    public static void addMetricNameConversion(String memqMetricName) {
        for (Map.Entry<String, String> prefixEntry : memqConsumerMetricsPrefixMap.entrySet()) {
            if (memqMetricName.startsWith(prefixEntry.getKey())) {
                memqConsumerMetricsMap.putIfAbsent(memqMetricName, memqMetricName.replace(prefixEntry.getKey(), prefixEntry.getValue()));
            }
        }
    }
}
