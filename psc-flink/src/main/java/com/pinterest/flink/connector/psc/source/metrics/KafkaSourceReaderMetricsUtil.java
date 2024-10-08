package com.pinterest.flink.connector.psc.source.metrics;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;

import java.util.Map;
import java.util.function.Predicate;

class KafkaSourceReaderMetricsUtil {

    // Kafka raw metric names and group names
    public static final String CONSUMER_FETCH_MANAGER_GROUP = "consumer-fetch-manager-metrics";
    public static final String BYTES_CONSUMED_TOTAL = "bytes-consumed-total";
    public static final String RECORDS_LAG = "records-lag-max";

    protected static Predicate<Map.Entry<MetricName, ? extends Metric>> createRecordLagFilter(TopicUriPartition tp) {
        final String resolvedTopic = tp.getTopicUriAsString().replace('.', '_');
        final String resolvedPartition = String.valueOf(tp.getPartition());
        return entry -> {
            final MetricName metricName = entry.getKey();
            final Map<String, String> tags = metricName.tags();

            return metricName.group().equals(CONSUMER_FETCH_MANAGER_GROUP)
                    && metricName.name().equals(RECORDS_LAG);
//                    && tags.containsKey("topic")
//                    && tags.get("topic").equals(resolvedTopic)
//                    && tags.containsKey("partition")
//                    && tags.get("partition").equals(resolvedPartition);
        };
    }

    protected static Predicate<Map.Entry<MetricName, ? extends Metric>> createBytesConsumedFilter() {
        return (entry) ->
                        entry.getKey().group().equals(CONSUMER_FETCH_MANAGER_GROUP)
                                && entry.getKey().name().equals(BYTES_CONSUMED_TOTAL)
                                && !entry.getKey().tags().containsKey("topic");
    }
}
