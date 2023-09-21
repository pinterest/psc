package com.pinterest.psc.metrics;

public class Metric {
    private final MetricName metricName;
    private final MetricValueProvider metricValueProvider;

    public Metric(MetricName metricName, MetricValueProvider metricValueProvider) {
        this.metricName = metricName;
        this.metricValueProvider = metricValueProvider;
    }

    public MetricName metricName() {
        return metricName;
    }

    public Object metricValue() {
        return metricValueProvider.getValue(metricName);
    }
}
