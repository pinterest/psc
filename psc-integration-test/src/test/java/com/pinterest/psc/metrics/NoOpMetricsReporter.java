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
import com.pinterest.psc.logging.PscLogger;
import io.vavr.Tuple4;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class NoOpMetricsReporter extends MetricsReporter {
    private static final PscLogger logger = PscLogger.getLogger(NoOpMetricsReporter.class);
    private final MetricTags metricTags;
    private List<Tuple4<String, Integer, Double, MetricTags>> buffer = new ArrayList<>();

    protected NoOpMetricsReporter(String baseName,
                                  PscMetricTag pscMetricTag,
                                  MetricRegistry registry,
                                  MetricFilter filter,
                                  TimeUnit rateUnit,
                                  TimeUnit durationUnit) {
        super(registry, pscMetricTag.getId(), filter, rateUnit, durationUnit);
        metricTags = new MetricTags(
                pscMetricTag.getHostname(),
                pscMetricTag.getHostIp(),
                pscMetricTag.getLocality(),
                pscMetricTag.getInstanceType(),
                pscMetricTag.getProcessId(),
                pscMetricTag.getThreadId(),
                pscMetricTag.getId(),
                pscMetricTag.getProject(),
                pscMetricTag.getVersion(),
                pscMetricTag.getAdditionalTags()
        );
    }

    private static String sanitize(String tagValue) {
        return tagValue == null ? "null" : tagValue.replace(":", "/").replace("+", "/");
    }

    public static NoOpMetricsReporter createReporter(MetricsReporterConfiguration metricsReporterConfiguration,
                                                     String baseName,
                                                     PscMetricTag pscMetricTag,
                                                     MetricRegistry registry,
                                                     MetricFilter filter,
                                                     TimeUnit rateUnit,
                                                     TimeUnit durationUnit) throws UnknownHostException {
        return new NoOpMetricsReporter(baseName, pscMetricTag, registry, filter, rateUnit, durationUnit);
    }

    @Override
    public void report(@SuppressWarnings("rawtypes") SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        try {
            int epochSecs = (int) (System.currentTimeMillis() / 1000);
            for (Entry<String, Counter> entry : counters.entrySet()) {
                buffer.add(new Tuple4<>(entry.getKey(), epochSecs, 1.0 * entry.getValue().getCount(), metricTags));
            }

            for (Entry<String, Meter> entry : meters.entrySet()) {
                buffer.add(new Tuple4<>(entry.getKey(), epochSecs, 1.0 * entry.getValue().getCount(), metricTags));
            }

            for (Entry<String, Histogram> entry : histograms.entrySet()) {
                generateMetrics(buffer, epochSecs, entry.getKey(), entry.getValue().getSnapshot());
            }

            for (Entry<String, Timer> entry : timers.entrySet()) {
                generateMetrics(buffer, epochSecs, entry.getKey(), entry.getValue().getSnapshot());
            }

            for (@SuppressWarnings("rawtypes")
                    Entry<String, Gauge> entry : gauges.entrySet()) {
                if (entry.getValue().getValue() instanceof Long) {
                    buffer.add(new Tuple4<>(entry.getKey(), epochSecs, 1.0 * (Long) entry.getValue().getValue(), metricTags));
                } else if (entry.getValue().getValue() instanceof Double) {
                    buffer.add(new Tuple4<>(entry.getKey(), epochSecs, (Double) entry.getValue().getValue(), metricTags));
                } else {
                    String val = entry.getValue().getValue().toString();
                    if (!val.contains("["))
                        buffer.add(new Tuple4<>(entry.getKey(), epochSecs, Double.parseDouble(val), metricTags));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to write metrics.", e);
        }
    }

    private void generateMetrics(List<Tuple4<String, Integer, Double, MetricTags>> buffer, int epochSecs, String key, Snapshot snapshot) {
        buffer.add(new Tuple4<>(key + ".avg", epochSecs, snapshot.getMean(), metricTags));
        buffer.add(new Tuple4<>(key + ".min", epochSecs, 1.0 * snapshot.getMin(), metricTags));
        buffer.add(new Tuple4<>(key + ".median", epochSecs, snapshot.getMedian(), metricTags));
        buffer.add(new Tuple4<>(key + ".p50", epochSecs, snapshot.getMedian(), metricTags));
        buffer.add(new Tuple4<>(key + ".p75", epochSecs, snapshot.get75thPercentile(), metricTags));
        buffer.add(new Tuple4<>(key + ".p95", epochSecs, snapshot.get95thPercentile(), metricTags));
        buffer.add(new Tuple4<>(key + ".p98", epochSecs, snapshot.get98thPercentile(), metricTags));
        buffer.add(new Tuple4<>(key + ".p99", epochSecs, snapshot.get99thPercentile(), metricTags));
        buffer.add(new Tuple4<>(key + ".p999", epochSecs, snapshot.get999thPercentile(), metricTags));
        buffer.add(new Tuple4<>(key + ".max", epochSecs, 1.0 * snapshot.getMax(), metricTags));
        buffer.add(new Tuple4<>(key + ".stddev", epochSecs, snapshot.getStdDev(), metricTags));
    }

    public List<Tuple4<String, Integer, Double, MetricTags>> getMetrics() {
        return buffer;
    }
}