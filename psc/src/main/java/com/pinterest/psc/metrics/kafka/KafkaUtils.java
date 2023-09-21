package com.pinterest.psc.metrics.kafka;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.metrics.MetricValueProvider;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;

public class KafkaUtils {
    public static void convertKafkaMetricsToPscMetrics(
            Map<MetricName, ? extends Metric> kafkaMetrics,
            MetricValueProvider metricValueProvider
    ) {
        if (kafkaMetrics == null)
            return;

        metricValueProvider.reset();
        kafkaMetrics.forEach((key, value) -> {
            Map<String, String> tags = key.tags();
            tags.put("backend", PscUtils.BACKEND_TYPE_KAFKA);
            com.pinterest.psc.metrics.MetricName metricName = new com.pinterest.psc.metrics.MetricName(
                    key.name(), key.group(), key.description(), tags
            );
            metricValueProvider.updateMetric(metricName, value.metricValue());
        });
    }
}
