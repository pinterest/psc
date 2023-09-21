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

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class OpenTSDBReporter extends MetricsReporter {
    private static final PscLogger logger = PscLogger.getLogger(OpenTSDBReporter.class);
    private static AtomicReference<OpenTSDBClient> openTSDBClient = new AtomicReference<>();

    private final String hostnameTag;
    private final String hostIpTag;
    private final String localityTag;
    private final String instanceTypeTag;
    private final String processIdTag;
    private final String threadIdTag;
    private final String uriTag;
    private final String projectTag;
    private final String versionTag;
    private final String otherTags;
    private final String baseName;

    // Debug
    private final PscMetricTag pscMetricTag;

    protected OpenTSDBReporter(String baseName,
                               PscMetricTag pscMetricTag,
                               MetricRegistry registry,
                               MetricFilter filter,
                               TimeUnit rateUnit,
                               TimeUnit durationUnit) {
        super(registry, pscMetricTag.getId(), filter, rateUnit, durationUnit);

        this.pscMetricTag = pscMetricTag;

        if (baseName == null || baseName.isEmpty()) {
            this.baseName = "";
        } else {
            this.baseName = baseName + ".";
        }

        this.uriTag = "uri=" + sanitize(pscMetricTag.getId());
        this.hostnameTag = "hostname=" + sanitize(pscMetricTag.getHostname());
        this.hostIpTag = "ip=" + sanitize(pscMetricTag.getHostIp());
        this.localityTag = "locality=" + sanitize(pscMetricTag.getLocality());
        this.instanceTypeTag = "instance_type=" + sanitize(pscMetricTag.getInstanceType());
        this.processIdTag = "process=" + sanitize(pscMetricTag.getProcessId());
        this.threadIdTag = "thread=" + pscMetricTag.getThreadId();
        this.projectTag = "project=" + sanitize(pscMetricTag.getProject());
        this.versionTag = "version=" + sanitize(pscMetricTag.getVersion());

        if (pscMetricTag.getAdditionalTags() != null && !pscMetricTag.getAdditionalTags().isEmpty()) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<String, String> entry : pscMetricTag.getAdditionalTags().entrySet())
                stringBuilder.append(entry.getKey()).append("=").append(sanitize(entry.getValue())).append(" ");
            this.otherTags = stringBuilder.toString();
            logger.info("Created OpenTSDBReporter with uriTag: {}, additionalTags: {}", uriTag, otherTags);
        } else
            this.otherTags = "";
    }

    private static String sanitize(String tagValue) {
        return tagValue == null ? "null" : tagValue.replace(":", "/").replace("+", "/").replace("\"", "");
    }

    public static OpenTSDBReporter createReporter(MetricsReporterConfiguration metricsReporterConfiguration,
                                                  String baseName,
                                                  PscMetricTag pscMetricTag,
                                                  MetricRegistry registry,
                                                  MetricFilter filter,
                                                  TimeUnit rateUnit,
                                                  TimeUnit durationUnit) throws UnknownHostException {
        if (openTSDBClient.get() == null) {
            openTSDBClient.compareAndSet(
                    null,
                    new OpenTSDBClient(
                            metricsReporterConfiguration.getHost(),
                            metricsReporterConfiguration.getPort()
                    )
            );
        }
        return new OpenTSDBReporter(baseName, pscMetricTag, registry, filter, rateUnit, durationUnit);
    }

    @Override
    public void report(@SuppressWarnings("rawtypes") SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        try {
            int epochSecs = (int) (System.currentTimeMillis() / 1000);
            OpenTSDBClient.MetricsBuffer buffer = new OpenTSDBClient.MetricsBuffer("" + baseName);
            for (Entry<String, Counter> entry : counters.entrySet()) {
                buffer.addMetric(entry.getKey(), epochSecs, entry.getValue().getCount(), uriTag,
                        hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                        versionTag, otherTags);
            }

            for (Entry<String, Meter> entry : meters.entrySet()) {
                buffer.addMetric(entry.getKey(), epochSecs, entry.getValue().getCount(), uriTag,
                        hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                        versionTag, otherTags);
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
                    buffer.addMetric(entry.getKey(), epochSecs, (Long) entry.getValue().getValue(), uriTag,
                            hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                            versionTag, otherTags);
                } else if (entry.getValue().getValue() instanceof Double) {
                    buffer.addMetric(entry.getKey(), epochSecs, (Double) entry.getValue().getValue(), uriTag,
                            hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                            versionTag, otherTags);
                } else {
                    String val = entry.getValue().getValue().toString();
                    if (!val.contains("[")) {
                        buffer.addMetric(entry.getKey(), epochSecs, Double.parseDouble(val), uriTag,
                                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                                versionTag, otherTags);
                    }
                }
            }

            openTSDBClient.get().sendMetrics(buffer);
        } catch (Exception e) {
            logger.debug("Failed to write metrics to opentsdb, this is likely a transient issue", e);
        }
    }

    private void generateMetrics(OpenTSDBClient.MetricsBuffer buffer, int epochSecs, String key, Snapshot snapshot) {
        buffer.addMetric(key + ".avg", epochSecs, snapshot.getMean(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
        buffer.addMetric(key + ".min", epochSecs, snapshot.getMin(), uriTag, hostnameTag,
                hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag, versionTag,
                otherTags);
        buffer.addMetric(key + ".median", epochSecs, snapshot.getMedian(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
        buffer.addMetric(key + ".p50", epochSecs, snapshot.getMedian(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
        buffer.addMetric(key + ".p75", epochSecs, snapshot.get75thPercentile(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
        buffer.addMetric(key + ".p95", epochSecs, snapshot.get95thPercentile(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
        buffer.addMetric(key + ".p98", epochSecs, snapshot.get98thPercentile(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
        buffer.addMetric(key + ".p99", epochSecs, snapshot.get99thPercentile(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
        buffer.addMetric(key + ".p999", epochSecs, snapshot.get999thPercentile(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
        buffer.addMetric(key + ".max", epochSecs, snapshot.getMax(), uriTag, hostnameTag,
                hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag, versionTag,
                otherTags);
        buffer.addMetric(key + ".stddev", epochSecs, snapshot.getStdDev(), uriTag,
                hostnameTag, hostIpTag, localityTag, instanceTypeTag, processIdTag, threadIdTag, projectTag,
                versionTag, otherTags);
    }
}