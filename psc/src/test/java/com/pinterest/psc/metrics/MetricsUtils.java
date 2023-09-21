package com.pinterest.psc.metrics;

import com.pinterest.psc.config.PscConfigurationInternal;

public class MetricsUtils {
    public static void resetMetrics(PscMetricRegistryManager pscMetricRegistryManager) {
        pscMetricRegistryManager.cleanup();
        pscMetricRegistryManager.clearCurrentThreadMetricMap();
    }

    public static void shutdownMetrics(PscMetricRegistryManager pscMetricRegistryManager, PscConfigurationInternal pscConfigurationInternal) {
        pscMetricRegistryManager.shutdown(pscConfigurationInternal);
    }
}
