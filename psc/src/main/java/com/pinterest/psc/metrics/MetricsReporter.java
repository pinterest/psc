package com.pinterest.psc.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.config.MetricsReporterConfiguration;
import com.pinterest.psc.logging.PscLogger;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class MetricsReporter extends ScheduledReporter {
    private static final PscLogger logger = PscLogger.getLogger(MetricsReporter.class);
    private static final String PID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    private static final ThreadLocal<String> TID =
            ThreadLocal.withInitial(() -> String.valueOf(Thread.currentThread().getId()));
    private static final String HOSTNAME = PscCommon.getHostname();
    public static MetricsReporter createReporter(MetricsReporterConfiguration metricsReporterConfiguration,
                                                 String baseName,
                                                 PscMetricTag pscMetricTag,
                                                 MetricRegistry registry,
                                                 MetricFilter filter,
                                                 TimeUnit rateUnit,
                                                 TimeUnit durationUnit) throws UnknownHostException {
        return null;
    }

    protected MetricsReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
    }

    protected MetricsReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, ScheduledExecutorService executor) {
        super(registry, name, filter, rateUnit, durationUnit, executor);
    }

    protected MetricsReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, ScheduledExecutorService executor, boolean shutdownExecutorOnStop) {
        super(registry, name, filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop);
    }

    protected MetricsReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, ScheduledExecutorService executor, boolean shutdownExecutorOnStop, Set<MetricAttribute> disabledMetricAttributes) {
        super(registry, name, filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop, disabledMetricAttributes);
    }

    public final void report() {
        logger.addContext("pscPid", PID);
        logger.addContext("pscTid", TID.get());
        logger.addContext("pscHost", HOSTNAME);
        super.report();
    }

    public abstract void report(
            SortedMap<String, Gauge> gauges,
            SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms,
            SortedMap<String, Meter> meters,
            SortedMap<String, Timer> timers
    );

}
