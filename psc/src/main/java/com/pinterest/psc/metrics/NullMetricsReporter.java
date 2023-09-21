package com.pinterest.psc.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.pinterest.psc.config.MetricsReporterConfiguration;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class NullMetricsReporter extends MetricsReporter {

    protected NullMetricsReporter(
            String baseName,
            PscMetricTag pscMetricTag,
            MetricRegistry registry,
            MetricFilter filter,
            TimeUnit rateUnit,
            TimeUnit durationUnit) {
        super(registry, pscMetricTag.getId(), filter, rateUnit, durationUnit);
    }

    public static NullMetricsReporter createReporter(MetricsReporterConfiguration metricsReporterConfiguration,
                                                     String baseName,
                                                     PscMetricTag pscMetricTag,
                                                     MetricRegistry registry,
                                                     MetricFilter filter,
                                                     TimeUnit rateUnit,
                                                     TimeUnit durationUnit) {
        return new NullMetricsReporter(baseName, pscMetricTag, registry, filter, rateUnit, durationUnit);
    }

    @Override
    public void report(@SuppressWarnings("rawtypes") SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
    }

}