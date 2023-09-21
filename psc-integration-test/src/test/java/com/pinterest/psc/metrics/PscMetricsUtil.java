package com.pinterest.psc.metrics;

import com.codahale.metrics.ScheduledReporter;

import java.util.Map;

public class PscMetricsUtil {

    public static boolean isInitializationError(PscMetricRegistryManager pscMetricRegistryManager) {
        return pscMetricRegistryManager.isInitializationError();
    }

    public static Map<String, ScheduledReporter> getPscMetricReporterMap(PscMetricRegistryManager pscMetricRegistryManager) {
        return pscMetricRegistryManager.getPscMetricReporterMap();
    }

    public static void cleanup(PscMetricRegistryManager pscMetricRegistryManager) {
        pscMetricRegistryManager.cleanup();
        pscMetricRegistryManager.clearCurrentThreadMetricMap();
    }
}
