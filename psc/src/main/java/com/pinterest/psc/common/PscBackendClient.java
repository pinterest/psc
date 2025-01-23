package com.pinterest.psc.common;

import com.pinterest.psc.common.event.PscEvent;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.MetricValueProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class PscBackendClient<K, V> implements AutoCloseable {
    private static final PscLogger logger = PscLogger.getLogger(PscBackendClient.class);
    protected final Map<String, TopicUri> backendTopicToTopicUri = new ConcurrentHashMap<>();
    protected final OverwriteSet activeTopicUrisOrPartitions = new OverwriteSet();
    protected final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    protected final MetricValueProvider metricValueProvider = new MetricValueProvider();
    protected boolean autoResolutionEnabled = true;
    protected int autoResolutionRetryCount = 5; // number of times an API call is retried when needed
    protected final static int DEFAULT_BACKOFF_FACTOR_MS = 200; // formula: 2^retry_num * backoff_ms
    protected int retries = 0;

    /**
     * Implement a simple exponential wait before the next try of a backend client call.
     *
     * @return true if backoff is performed, false if all retries are exhausted
     */
    protected boolean backoff() {
        retries = backoff(retries);
        return retries <= autoResolutionRetryCount;
    }

    protected int backoff(int retries) {
        if (++retries > autoResolutionRetryCount)
            return retries;

        try {
            long backoffPeriodMs = (long) Math.pow(2, retries) * DEFAULT_BACKOFF_FACTOR_MS;
            Thread.sleep(backoffPeriodMs);
            logger.info("Backed off for {} ms, before retrying (#{}) the call.", backoffPeriodMs, retries);
        } catch (InterruptedException e) {
            // unexpected
            logger.warn("Failed to apply backoff", e);
        }
        return retries;
    }

    /**
     * @return the PSC configuration used to create the backend client (producer or consumer).
     */
    public abstract PscConfiguration getConfiguration();

    /**
     * Returns the PSC-converted metrics associated with the backend client (producer or consumer).
     *
     * @return PSC-converted metrics of the backend consumer.
     * @throws ClientException if there are validation issues or backend failures.
     */
    public abstract Map<MetricName, ? extends Metric> metrics() throws ClientException;

    /**
     * Implements any global reset action for backend producers and consumers
     *
     * @throws ClientException if reset fails
     */
    protected void resetBackendClient() throws ClientException { }

    /**
     * Implements and global retry action for backend producers and consumers APIs
     */
    protected void retryBackendClient() { }

    /**
     * Allows event-based logic to be executed by the backend producer/consumer
     * @param event an PscEvent that the consumer may handle
     */
    public void onEvent(PscEvent event) {

    }
}
