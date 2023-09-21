package com.pinterest.psc.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.MetricsReporterConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationReporter;
import com.pinterest.psc.logging.PscLogger;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * API's related to metric operations are defined here and used throughout PSC codebase. The {@link PscMetricRegistryManager}
 * is initialized as a singleton object and runs in a separate threadpool
 * until the client is closed via {@link PscMetricRegistryManager#shutdown(PscConfigurationInternal)}.
 */
public class PscMetricRegistryManager {
    private static final PscLogger logger = PscLogger.getLogger(PscMetricRegistryManager.class);
    private static final PscMetricRegistryManager singletonPscMetricRegistryManager = new PscMetricRegistryManager();
    private static PscMetricTagManager pscMetricTagManager;
    private final ThreadLocal<Map<String, PscMetricRegistryAndReporter>> pscMetricRegistryAndReporterMap = ThreadLocal.withInitial(HashMap::new);
    private boolean initializationError = false;
    private ScheduledExecutorService executorService;
    private final AtomicInteger refCount = new AtomicInteger(0);

    private PscMetricRegistryManager() {
    }

    @VisibleForTesting
    protected void cleanup() {
        PscMetricRegistryAndReporter.cleanup();
        if (pscMetricTagManager != null)
            pscMetricTagManager.cleanup();
    }


    @VisibleForTesting
    // for testing only
    protected void clearCurrentThreadMetricMap() {
        pscMetricRegistryAndReporterMap.get().clear();
    }

    @VisibleForTesting
    protected boolean isInitializationError() {
        return initializationError;
    }

    /**
     * @return the singleton instance of {@link PscMetricRegistryManager}
     */
    public static PscMetricRegistryManager getInstance() {
        return singletonPscMetricRegistryManager;
    }

    /**
     * If reporting is disabled, this is a no-op. Otherwise, initialize the {@link PscMetricTagManager} and
     * thread pool used for metrics.
     * @param pscConfigurationInternal
     */
    public void initialize(PscConfigurationInternal pscConfigurationInternal) {
        if (!pscConfigurationInternal.getMetricsReporterConfiguration().isReportingEnabled())
            return;

        pscMetricTagManager = PscMetricTagManager.getInstance();

        if (!PscConfigurationReporter.isThisYou(pscConfigurationInternal.getConfiguration()))
            pscMetricTagManager.initializePscMetricTagManager(pscConfigurationInternal);

        // initialize the thread pool only once
        if (refCount.getAndIncrement() == 0)
            executorService = Executors.newScheduledThreadPool(pscConfigurationInternal.getMetricsReporterConfiguration().getReporterParallelism());

        logger.debug("PscMetricRegistryManager refcount incremented to {}.", refCount.get());
    }

    /**
     * Given a {@link PscMetricTag}, get the existing {@link MetricRegistry} for that metric tag or create a new one.
     * @param pscMetricTag
     * @param metricsReporterConfiguration
     * @return the {@link MetricRegistry} assigned to the {@link PscMetricTag}
     */
    public MetricRegistry getOrCreateMetricRegistry(PscMetricTag pscMetricTag, MetricsReporterConfiguration metricsReporterConfiguration) {
        if (pscMetricTag == null)
            return null;

        String key = serializeTag(pscMetricTag);

        if (PscMetricRegistryAndReporter.getGlobalRegistryAndReporterMap().get(key) != null) {
            return PscMetricRegistryAndReporter.getGlobalRegistryAndReporterMap().get(key).getRegistry();
        }

        final AtomicBoolean initializedNewConfiguredReporter = new AtomicBoolean();

        final PscMetricRegistryAndReporter metricRegistryAndReporter = pscMetricRegistryAndReporterMap.get()
            .computeIfAbsent(key, k -> {
                MetricRegistry registry = new MetricRegistry();
                return new PscMetricRegistryAndReporter(
                    key,
                    registry,
                    getMetricsReporter(
                        key,
                        pscMetricTag,
                        registry,
                        initializedNewConfiguredReporter,
                        metricsReporterConfiguration
                    )
                );
            });
        return metricRegistryAndReporter.getRegistry();
    }

    private MetricsReporter getMetricsReporter(String key, PscMetricTag pscMetricTag, MetricRegistry metricRegistry, AtomicBoolean initializedNewConfiguredReporter, MetricsReporterConfiguration metricsReporterConfiguration) {
        if (metricsReporterConfiguration == null)
            return null;

        initializedNewConfiguredReporter.set(false);
        MetricsReporter tmpReporter;
        try {
            Class<? extends MetricsReporter> metricsReporterClass = Class.forName(metricsReporterConfiguration.getReporterClass()).asSubclass(MetricsReporter.class);

            Method createReporterMethod = metricsReporterClass.getMethod("createReporter",
                    MetricsReporterConfiguration.class,
                    String.class,
                    PscMetricTag.class,
                    MetricRegistry.class,
                    MetricFilter.class,
                    TimeUnit.class,
                    TimeUnit.class
            );
            tmpReporter = (MetricsReporter) createReporterMethod.invoke(
                    null,
                    metricsReporterConfiguration,
                    PscMetrics.PSC_METRICS_REGISTRY_BASE_NAME,
                    pscMetricTag,
                    metricRegistry,
                    (MetricFilter) (String name, Metric metric) -> true,
                    TimeUnit.SECONDS,
                    TimeUnit.SECONDS
            );
            initializedNewConfiguredReporter.set(true);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            if (e instanceof ClassNotFoundException) {
                logger.error("Could not instantiate class '{}'; defaulting to null reporter",
                        metricsReporterConfiguration.getReporterClass(), e);
            } else if (e instanceof NoSuchMethodException) {
                logger.error("Could not find the expected static method 'createReporter' in class '{}'; " +
                                "defaulting to null reporter",
                        metricsReporterConfiguration.getReporterClass(), e);
            } else {
                logger.error("Error in creating the reporter object based on given metrics reporter configuration; " +
                        "defaulting to null reporter", e);
            }
            logger.info("Defaulting to null reporter", e);
            tmpReporter = new NullMetricsReporter(PscMetrics.PSC_METRICS_REGISTRY_BASE_NAME, pscMetricTag,
                    metricRegistry, (String name, Metric metric) -> true, TimeUnit.SECONDS, TimeUnit.SECONDS);
        }

        MetricsReporter reporter = tmpReporter;
        int frequencyMs = metricsReporterConfiguration.getFrequencyMs();
        executorService.scheduleAtFixedRate(
                reporter::report,
                Math.abs(key.hashCode()) % frequencyMs,
                frequencyMs,
                TimeUnit.MILLISECONDS
        );
        return reporter;
    }

    private String serializeTag(PscMetricTag tag) {
        return tag.getSerializedString();
    }

    private String serializeTag(Map<String, String> sortedTag) {
        return sortedTag.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(" "));
    }

    private Map<String, String> deserializeTag(String serializedTag) {
        return Arrays.stream(serializedTag.split(" ")).map(split -> split.split("="))
                .collect(Collectors.toMap(tag -> tag[0], tag -> tag[1]));
    }

    public void updateHistogramMetric(TopicUri topicUri, String metricKey, long metricValue, PscConfigurationInternal pscConfigurationInternal) {
        updateHistogramMetric(topicUri, PscUtils.NO_PARTITION, metricKey, metricValue, pscConfigurationInternal);
    }

    public void updateHistogramMetric(TopicUri topicUri,
                                      int partition,
                                      String metricKey,
                                      long metricValue,
                                      PscConfigurationInternal pscConfigurationInternal) {
        if (pscConfigurationInternal == null || pscConfigurationInternal.getMetricsReporterConfiguration() == null) {
            return;
        }
        if (!pscConfigurationInternal.getMetricsReporterConfiguration().isReportingEnabled())
            return;
        try {
            PscMetricTag pscMetricTag = pscMetricTagManager.getOrCreatePscMetricTag(topicUri, partition, pscConfigurationInternal);
            MetricRegistry metricRegistry = getOrCreateMetricRegistry(pscMetricTag, pscConfigurationInternal.getMetricsReporterConfiguration());
            if (metricRegistry != null) {
                metricRegistry.histogram(metricKey,
                        () -> new Histogram(
                                new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)
                        )
                ).update(metricValue);
            }
        } catch (Exception exception) {
            logger.warn("Failed to update histogram metric {}: ", metricKey, exception);
        }
    }

    public void updateBackendHistogramMetric(TopicUri topicUri, String metricKey, long metricValue, PscConfigurationInternal pscConfigurationInternal) {
        updateBackendHistogramMetric(topicUri, PscUtils.NO_PARTITION, metricKey, metricValue, pscConfigurationInternal);
    }

    public void updateBackendHistogramMetric(TopicUri topicUri,
                                             int partition,
                                             String metricKey,
                                             long metricValue,
                                             PscConfigurationInternal pscConfigurationInternal) {
        updateHistogramMetric(
                topicUri, partition, PscMetrics.PSC_BACKEND_METRICS_PREFIX + metricKey, metricValue, pscConfigurationInternal
        );
    }

    public void incrementCounterMetric(TopicUri topicUri, String metricKey, PscConfigurationInternal pscConfigurationInternal) {
        incrementCounterMetric(topicUri, metricKey, 1, pscConfigurationInternal);
    }

    public void incrementCounterMetric(TopicUri topicUri, String metricKey, long metricIncrease, PscConfigurationInternal pscConfigurationInternal) {
        incrementCounterMetric(topicUri, PscUtils.NO_PARTITION, metricKey, metricIncrease, pscConfigurationInternal);
    }

    public void incrementCounterMetric(TopicUri topicUri,
                                       int partition,
                                       String metricKey,
                                       PscConfigurationInternal pscConfigurationInternal) {
        incrementCounterMetric(topicUri, partition, metricKey, 1, pscConfigurationInternal);
    }

    public void incrementCounterMetric(TopicUri topicUri,
                                       int partition,
                                       String metricKey,
                                       long metricIncrease,
                                       PscConfigurationInternal pscConfigurationInternal) {
        if (pscConfigurationInternal == null || pscConfigurationInternal.getMetricsReporterConfiguration() == null) {
            return;
        }
        if (!pscConfigurationInternal.getMetricsReporterConfiguration().isReportingEnabled())
            return;
        try {
            PscMetricTag pscMetricTag = pscMetricTagManager.getOrCreatePscMetricTag(topicUri, partition, pscConfigurationInternal);
            MetricRegistry metricRegistry = getOrCreateMetricRegistry(pscMetricTag, pscConfigurationInternal.getMetricsReporterConfiguration());
            if (metricRegistry != null)
                metricRegistry.counter(metricKey).inc(metricIncrease);
        } catch (Exception exception) {
            logger.warn("Failed to increment counter metric {}: ", metricKey, exception);
        }
    }

    public void incrementBackendCounterMetric(TopicUri topicUri, String metricKey, PscConfigurationInternal pscConfigurationInternal) {
        incrementBackendCounterMetric(topicUri, metricKey, 1, pscConfigurationInternal);
    }

    public void incrementBackendCounterMetric(TopicUri topicUri, String metricKey, long metricIncrease, PscConfigurationInternal pscConfigurationInternal) {
        incrementBackendCounterMetric(topicUri, PscUtils.NO_PARTITION, metricKey, metricIncrease, pscConfigurationInternal);
    }

    public void incrementBackendCounterMetric(TopicUri topicUri,
                                              int partition,
                                              String metricKey,
                                              PscConfigurationInternal pscConfigurationInternal) {
        incrementBackendCounterMetric(topicUri, partition, metricKey, 1, pscConfigurationInternal);
    }

    public void incrementBackendCounterMetric(TopicUri topicUri,
                                              int partition,
                                              String metricKey,
                                              long metricIncrease,
                                              PscConfigurationInternal pscConfigurationInternal) {
        incrementCounterMetric(
                topicUri, partition, PscMetrics.PSC_BACKEND_METRICS_PREFIX + metricKey, metricIncrease, pscConfigurationInternal
        );
    }

    /**
     * Gracefully shuts down the metrics threadpool. Must be called whenever client is closed in order to ensure
     * graceful termination.
     * @param pscConfigurationInternal
     */
    public void shutdown(PscConfigurationInternal pscConfigurationInternal) {
        if (!pscConfigurationInternal.getMetricsReporterConfiguration().isReportingEnabled())
            return;

        if (refCount.decrementAndGet() == 0) {
            cleanup();
            executorService.shutdown();
        }
        logger.debug("PscMetricRegistryManager refcount decremented to {}.", refCount.get());
    }

    public void enableJvmMetrics(String registryId, PscConfigurationInternal pscConfigurationInternal) {
        if (pscConfigurationInternal == null) {
            logger.error("PscConfigurationInternal supplied is null");
            return;
        }
        if (PscConfigurationReporter.isThisYou(pscConfigurationInternal.getConfiguration())) {
            return;
        }
        if (!pscConfigurationInternal.getMetricsReporterConfiguration().isReportingEnabled())
            return;

        PscMetricTag pscMetricTag = pscMetricTagManager.getOrCreateBaseMetricTag(registryId, Thread.currentThread().getId(), pscConfigurationInternal);
        MetricRegistry jvmMetricRegistry = getOrCreateMetricRegistry(pscMetricTag, pscConfigurationInternal.getMetricsReporterConfiguration());
        if (jvmMetricRegistry != null && jvmMetricRegistry.getNames().isEmpty()) {
            try {
                jvmMetricRegistry.register("psc.gc", new GarbageCollectorMetricSet());
            } catch (IllegalArgumentException exception) {
                // already registered; skip
            }

            try {
                jvmMetricRegistry.register("psc.threads", new CachedThreadStatesGaugeSet(10, TimeUnit.SECONDS));
            } catch (IllegalArgumentException exception) {
                // already registered; skip
            }

            try {
                jvmMetricRegistry.register("psc.memory", new MemoryUsageGaugeSet());
            } catch (IllegalArgumentException exception) {
                // already registered; skip
            }
        }
    }

    public void reportToConsole() {
        PscMetricRegistryAndReporter.getGlobalRegistryAndReporterMap().values().forEach(rr -> {
            ConsoleReporter reporter = ConsoleReporter.forRegistry(rr.getRegistry()).build();
            reporter.start(1000, TimeUnit.MILLISECONDS);
            reporter.report();
            reporter.close();
        });
    }

    @VisibleForTesting
    public void setPscMetricTagManager(PscMetricTagManager pscMetricTagManager) {
        PscMetricRegistryManager.pscMetricTagManager = pscMetricTagManager;
    }

    public long getCounterMetric(TopicUri topicUri, String metricKey, PscConfigurationInternal pscConfigurationInternal) {
        return getCounterMetric(topicUri, PscUtils.NO_PARTITION, metricKey, pscConfigurationInternal);
    }

    public long getCounterMetric(TopicUri topicUri, int partition, String metricKey, PscConfigurationInternal pscConfigurationInternal) {
        PscMetricTag pscMetricTag = pscMetricTagManager.getOrCreatePscMetricTag(topicUri, partition, pscConfigurationInternal);
        MetricRegistry metricRegistry = getOrCreateMetricRegistry(pscMetricTag, null);
        return metricRegistry == null ? Long.MIN_VALUE : metricRegistry.counter(metricKey).getCount();
    }

    public long getBackendCounterMetric(TopicUri topicUri, String metricKey, PscConfigurationInternal pscConfigurationInternal) {
        return getBackendCounterMetric(topicUri, PscUtils.NO_PARTITION, metricKey, pscConfigurationInternal);
    }

    public long getBackendCounterMetric(TopicUri topicUri,
                                        int partition,
                                        String metricKey,
                                        PscConfigurationInternal pscConfigurationInternal) {
        return getCounterMetric(topicUri, partition, PscMetrics.PSC_BACKEND_METRICS_PREFIX + metricKey, pscConfigurationInternal);
    }

    public Snapshot getHistogramMetric(TopicUri topicUri, String metricKey, PscConfigurationInternal pscConfigurationInternal) {
        return getHistogramMetric(topicUri, PscUtils.NO_PARTITION, metricKey, pscConfigurationInternal);
    }

    public Snapshot getHistogramMetric(TopicUri topicUri,
                                       int partition,
                                       String metricKey,
                                       PscConfigurationInternal pscConfigurationInternal) {
        PscMetricTag pscMetricTag = pscMetricTagManager.getOrCreatePscMetricTag(topicUri, partition, pscConfigurationInternal);
        MetricRegistry metricRegistry = getOrCreateMetricRegistry(pscMetricTag, null);
        return metricRegistry == null ? null : metricRegistry.histogram(metricKey).getSnapshot();
    }

    public Snapshot getBackendHistogramMetric(TopicUri topicUri, String metricKey, PscConfigurationInternal pscConfigurationInternal) {
        return getBackendHistogramMetric(topicUri, PscUtils.NO_PARTITION, metricKey, pscConfigurationInternal);
    }

    public Snapshot getBackendHistogramMetric(TopicUri topicUri,
                                              int partition,
                                              String metricKey,
                                              PscConfigurationInternal pscConfigurationInternal) {
        return getHistogramMetric(topicUri, partition, PscMetrics.PSC_BACKEND_METRICS_PREFIX + metricKey, pscConfigurationInternal);
    }

    @VisibleForTesting
    protected Map<String, ScheduledReporter> getPscMetricReporterMap() {
        return PscMetricRegistryAndReporter.getGlobalRegistryAndReporterMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().reporter));
    }

    private static class PscMetricRegistryAndReporter implements Closeable {
        private static final Map<String, PscMetricRegistryAndReporter> globalRegistryAndReporterMap = new ConcurrentHashMap<>();
        private final String key;
        private final MetricRegistry registry;
        private final ScheduledReporter reporter;

        PscMetricRegistryAndReporter(String key,
                                            MetricRegistry registry,
                                            ScheduledReporter reporter) {
            this.key = key;
            this.registry = registry;
            this.reporter = reporter;
            globalRegistryAndReporterMap.put(key, this);

        }

        @Override
        public void close() {
            registry.removeMatching(MetricFilter.ALL);
            if (reporter != null)
                reporter.close();
            globalRegistryAndReporterMap.remove(key);
        }

        protected MetricRegistry getRegistry() {
            return registry;
        }

        protected static Map<String, PscMetricRegistryAndReporter> getGlobalRegistryAndReporterMap() {
            return globalRegistryAndReporterMap;
        }

        @VisibleForTesting
        protected static void cleanup() {
            globalRegistryAndReporterMap.values().forEach(PscMetricRegistryAndReporter::close);
            globalRegistryAndReporterMap.clear();
        }
    }
}
