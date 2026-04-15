package com.pinterest.flink.connector.psc.source.metrics;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;

import java.util.Map;
import java.util.function.Predicate;

class MemqSourceReaderMetricsUtil {

    public static final String MEMQ_CONSUMER_METRIC_GROUP = "memq-consumer-metrics";
    public static final String BYTES_CONSUMED_TOTAL = "bytes.consumed.total";
    public static final String NOTIFICATION_RECORDS_LAG_MAX = "notification.records.lag.max";

    protected static Predicate<Map.Entry<MetricName, ? extends Metric>> createBytesConsumedFilter() {
        return entry ->
                entry.getKey().group().equals(MEMQ_CONSUMER_METRIC_GROUP)
                        && entry.getKey().name().equals(BYTES_CONSUMED_TOTAL);
    }

    protected static Predicate<Map.Entry<MetricName, ? extends Metric>> createRecordsLagFilter(TopicUriPartition tp) {
        return entry ->
                entry.getKey().group().equals(MEMQ_CONSUMER_METRIC_GROUP)
                        && entry.getKey().name().equals(NOTIFICATION_RECORDS_LAG_MAX);
    }
}
