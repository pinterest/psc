package com.pinterest.psc.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MetricValueProvider {
    private final Map<MetricName, Object> internalMetrics = new HashMap<>();
    private final Map<MetricName, Metric> externalMetrics = new HashMap<>();

    public MetricValueProvider() {
    }

    public void updateMetrics(Map<MetricName, Object> metrics) {
        reset();
        internalMetrics.putAll(metrics);
        externalMetrics.putAll(metrics.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new Metric(entry.getKey(), this)
        )));
    }

    public void reset() {
        internalMetrics.clear();
        externalMetrics.clear();
    }

    public void updateMetric(MetricName metricName, Object metric) {
        internalMetrics.put(metricName, metric);
        if (!externalMetrics.containsKey(metricName))
            externalMetrics.put(metricName, new Metric(metricName, this));
    }

    public Object getValue(MetricName metricName) {
        return internalMetrics.get(metricName);
    }

    public Map<MetricName, Metric> getMetrics() {
        return externalMetrics;
    }
}
