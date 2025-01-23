package com.pinterest.psc.metrics;

import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSubImpl;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.util.InstantiationUtil;

import java.util.concurrent.atomic.AtomicInteger;

public class PscMetricsUtils {
    private static AtomicInteger hitCounter = new AtomicInteger(0);
    private static PscConfigurationInternal pscConfigurationInternal;

    public static PscConfigurationInternal initializePscMetrics(Boolean enableMetricReporting) {
        if (hitCounter.getAndIncrement() == 0) {
            try {
                Class<?> clazz = Class.forName(PscTestEnvironmentWithKafkaAsPubSubImpl.class.getName());
                PscTestEnvironmentWithKafkaAsPubSub pscTestEnvWithKafka = (PscTestEnvironmentWithKafkaAsPubSub) InstantiationUtil.instantiate(clazz);
                pscTestEnvWithKafka.prepare(PscTestEnvironmentWithKafkaAsPubSub.createConfig());
                PscConfiguration pscConfiguration = new PscConfiguration();
                pscTestEnvWithKafka.getStandardPscConsumerConfiguration().keySet().stream().map(Object::toString)
                        .filter(key -> key.startsWith("psc.")).forEach(
                                key -> pscConfiguration.setProperty(
                                        key, pscTestEnvWithKafka.getStandardPscConsumerConfiguration().get(key)
                                )
                        );

                if (enableMetricReporting != null) {
                    pscConfiguration.setProperty(PscConfiguration.PSC_METRIC_REPORTING_ENABLED, enableMetricReporting);
                }

                pscConfigurationInternal = new PscConfigurationInternal(pscConfiguration, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER, true);
                PscMetricRegistryManager.getInstance().initialize(
                        pscConfigurationInternal
                );
                cleanupPscMetrics();
                pscTestEnvWithKafka.shutdown();
            } catch (ConfigurationException configurationException) {
                throw new IllegalArgumentException(configurationException);
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }
        return pscConfigurationInternal;
    }

    public static PscConfigurationInternal initializePscMetrics() {
        return initializePscMetrics(null);
    }

    public static void shutdownPscMetrics() {
        if (hitCounter.decrementAndGet() == 0) {
            PscMetricRegistryManager.getInstance().shutdown(pscConfigurationInternal);
        }
    }

    public static void cleanupPscMetrics() {
        PscMetricRegistryManager.getInstance().cleanup();
        PscMetricRegistryManager.getInstance().clearCurrentThreadMetricMap();
    }
}
