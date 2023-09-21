package com.pinterest.psc.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.pinterest.psc.config.MetricsReporterConfiguration;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class ConsoleMetricsReporter extends MetricsReporter {

    private final String uriTag;

    protected ConsoleMetricsReporter(
            String baseName,
            PscMetricTag pscMetricTag,
            MetricRegistry registry,
            MetricFilter filter,
            TimeUnit rateUnit,
            TimeUnit durationUnit) {
        super(registry, pscMetricTag.getId(), filter, rateUnit, durationUnit);
        this.uriTag = "uri=" + pscMetricTag.getId();
    }

    public static ConsoleMetricsReporter createReporter(
            MetricsReporterConfiguration metricsReporterConfiguration,
            String baseName,
            PscMetricTag pscMetricTag,
            MetricRegistry registry,
            MetricFilter filter,
            TimeUnit rateUnit,
            TimeUnit durationUnit) {
        return new ConsoleMetricsReporter(baseName, pscMetricTag, registry, filter, rateUnit, durationUnit);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        long epochSecs = System.currentTimeMillis();
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            System.out.println(epochSecs + " " + entry.getKey() + " " + uriTag + " " + entry.getValue().getCount());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            System.out.println(epochSecs + " " + entry.getKey() + " " + uriTag + " " + entry.getValue().getMeanRate());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            generateMetrics(epochSecs, entry.getKey(), entry.getValue().getSnapshot());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            generateMetrics(epochSecs, entry.getKey(), entry.getValue().getSnapshot());
        }

        for (@SuppressWarnings("rawtypes")
                Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            System.out.println(epochSecs + " " + entry.getKey() + " " + uriTag + " " + entry.getValue().getValue());
        }
    }

    private void generateMetrics(long epochSecs, String key, Snapshot snapshot) {
        System.out.println(epochSecs + " " + key + ".avg " + uriTag + " " + snapshot.getMean());
        System.out.println(epochSecs + " " + key + ".min " + uriTag + " " + snapshot.getMin());
        System.out.println(epochSecs + " " + key + ".max " + uriTag + " " + snapshot.getMax());
    }
}
