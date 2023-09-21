package com.pinterest.psc.producer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscBackendClient;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaErrors;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.exception.producer.BackendProducerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.interceptor.ProducerInterceptors;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Objects of this class type are intended to act as mediators between {@link PscProducer} and backend client library
 * producers (e.g. {@link org.apache.kafka.clients.producer.KafkaProducer}. They are responsible to convert their API
 * calls to proper API calls that are supported by their backend client library. PSC producer creates one object of
 * this class per backend type and backend cluster pair.
 *
 * @param <K> the key type of PSC messages as configured by the user.
 * @param <V> the value type of PSC messages as configured by the user.
 */
public abstract class PscBackendProducer<K, V> extends PscBackendClient<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(PscBackendProducer.class);
    protected ProducerInterceptors<K, V> producerInterceptors;
    protected PscConfigurationInternal pscConfigurationInternal;
    protected Environment environment;
    protected Class lastExceptionClass = null;

    /**
     * Performs common initializes across all backend consumer types, and delegates backend-specific initializations.
     *
     * @param pscConfigurationInternal the PSC producer configuration that should be converted to backend specific producer
     *                         configuration using
     *                         {@link com.pinterest.psc.config.PscProducerToBackendProducerConfigConverter}.
     * @param discoveryConfig  the service discovery configuration is used to configure the endpoint for the backend
     *                         producer.
     * @param topicUri         the topicUri the producer is producing to. Since a PSC producer instance can launch multiple
     *                         backend producers the client id configured by the user needs to create unique client ids to avoid
     *                         name clashes with metrics by the backend producer. For this, PSC appends the backend type and
     *                         cluster to the provided client id.
     */
    public void initialize(
            PscConfigurationInternal pscConfigurationInternal, ServiceDiscoveryConfig discoveryConfig, Environment environment, TopicUri topicUri
    ) {
        this.autoResolutionEnabled = pscConfigurationInternal.isAutoResolutionEnabled();
        this.autoResolutionRetryCount = pscConfigurationInternal.getAutoResolutionRetryCount();
        this.pscConfigurationInternal = pscConfigurationInternal;
        scheduler.scheduleAtFixedRate(
                this::reportProducerMetrics,
                new Random().nextInt(pscConfigurationInternal.getConfiguration().getInt(PscConfiguration.PSC_METRICS_FREQUENCY_MS)),
                pscConfigurationInternal.getConfiguration().getInt(PscConfiguration.PSC_METRICS_FREQUENCY_MS),
                TimeUnit.MILLISECONDS
        );
        this.environment = environment;
    }

    /**
     * Handles reporting metrics from the backend consumer. This method should be overloaded by specific backend clients
     * to implement proper metrics reporting.
     */
    protected synchronized void reportProducerMetrics() {
    }

    /**
     * @param topicUri the topic URI for which corresponding partitions are requested.
     * @return a set of topic URI partitions corresponding to the given topic URI.
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    public abstract Set<TopicUriPartition> getPartitions(TopicUri topicUri) throws ProducerException;

    /**
     * Sends a PSC producer message to the backend pubsub cluster. If a callback is provided, once the send is complete
     * and acked by the backend the callback will be triggered.
     *
     * @param pscProducerMessage an individual PSC producer message to send to backend.
     * @param callback           the object to call when the send is complete.
     * @return a message id future that has information about the persisted message.
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    public abstract Future<MessageId> send(PscProducerMessage<K, V> pscProducerMessage, Callback callback) throws ProducerException;

    /**
     * Sends the given topic URI partition offsets to be committed against the given consumer group.
     *
     * @param offsetByTopicUriPartition
     * @param consumerGroupId
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    public abstract void sendOffsetsToTransaction(Map<TopicUriPartition, Long> offsetByTopicUriPartition, String consumerGroupId) throws ProducerException;

    /**
     * This causes all buffered messages to be sent immediately and blocks until their send completion.
     *
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    public abstract void flush() throws ProducerException;

    /**
     * Closes the backend producer object.
     *
     * @param duration the maximum amount of time the call should wait for the backend producer close to complete.
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    public void close(Duration duration) throws ProducerException {
        scheduler.shutdown();
    }

    /**
     * Aborts an active transaction.
     *
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    @InterfaceStability.Evolving
    public abstract void abortTransaction() throws ProducerException;

    /**
     * Begins a transaction.
     *
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    @InterfaceStability.Evolving
    public abstract void beginTransaction() throws ProducerException;

    /**
     * Commits an active transaction.
     *
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    @InterfaceStability.Evolving
    public abstract void commitTransaction() throws ProducerException;

    /**
     * Initializes the backend producer for transactions.
     *
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    @InterfaceStability.Evolving
    public abstract void initTransaction() throws ProducerException;

    /**
     * Resumes an in progress transaction that another producer was handling.
     *
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    @InterfaceStability.Evolving
    public abstract void resumeTransaction(PscBackendProducer otherBackendProducer) throws ProducerException;

    /**
     * Resumes a transaction from a stored transaction state.
     *
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    @InterfaceStability.Evolving
    public abstract void resumeTransaction(PscProducerTransactionalProperties pscProducerTransactionalProperties) throws ProducerException;

    /**
     * Returns transactional properties of the backend producer.
     *
     * @return transactional properties that identify the transaction.
     * @throws ProducerException if there are validation issues or backend failures.
     */
    @InterfaceStability.Evolving
    public abstract PscProducerTransactionalProperties getTransactionalProperties() throws ProducerException;

    /**
     * @return the transaction manager object associated with this producer.
     * @throws ProducerException if there are validation issues or retrieval fails.
     */
    @InterfaceStability.Evolving
    public abstract Object getTransactionManager() throws ProducerException;

    /**
     * Interrupts any ongoing sends that may be in progress by this backend producer.
     *
     * @throws ProducerException if there are validation issues or exceptions thrown from the backend call.
     */
    public abstract void wakeup() throws ProducerException;

    /**
     * Sets the producer interceptors (both default and user defined) only when creating the consumer.
     *
     * @param producerInterceptors the interceptors to assign to this producer.
     */
    public final void setProducerInterceptors(ProducerInterceptors<K, V> producerInterceptors) {
        this.producerInterceptors = producerInterceptors;
    }

    /**
     * Performs preparations for Throwing the proper PSC producer exception based on the exception coming from the
     * backend client.
     * @param backendException the exception from the backend client
     * @param topicUrisOrPartitions involved topic URIs or topic URI partitions
     * @param emitMetrics whether to emit metrics
     * @param producerExceptionClass the PSC producer exception class to throw
     * @throws ProducerException if the exception cannot be handled internally
     */
    protected void handleException(Exception backendException, Set topicUrisOrPartitions, boolean emitMetrics, Class<? extends ProducerException> producerExceptionClass) throws ProducerException {
        PscErrorHandler.handle(
                new BackendProducerException(backendException, PscUtils.BACKEND_TYPE_KAFKA),
                topicUrisOrPartitions,
                emitMetrics, pscConfigurationInternal
        );
        retries = 0;

        try {
            throw producerExceptionClass.getConstructor(Throwable.class).newInstance(backendException);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new ProducerException(
                    String.format(
                            "Failed to instantiate from the exception class '%s', throwing the generic ProducerException instead.",
                            producerExceptionClass.getName()
                    ),
                    backendException
            );
        }
    }

    /**
     * Depending on the thrown exception from the backend client, it decides about the actions to take.
     * @param exception the exception from the backend client
     * @param topicUrisOrPartitions involved topic URIs or topic URI partitions
     * @param emitMetrics whether to emit metrics
     * @throws ProducerException the superclass of the to-be-thrown exception
     */
    protected void handleException(Exception exception, Set topicUrisOrPartitions, boolean emitMetrics) throws ProducerException {
        logger.warn("Exception {} caught from the backend producer:", exception.getClass().getName(), exception);
        PscErrorHandler.ProducerAction followupAction = KafkaErrors.shouldHandleProducerException(exception, autoResolutionEnabled);
        logger.info("Auto resolution status: {} (remaining retries: {})", autoResolutionEnabled, autoResolutionRetryCount - retries);
        logger.info("Follow up action to this exception: {}", followupAction.actionType);

        if (lastExceptionClass != exception.getClass()) {
            lastExceptionClass = exception.getClass();
            retries = 0;
        }

        switch (followupAction.actionType) {
            case THROW:
                handleException(exception, topicUrisOrPartitions, emitMetrics, followupAction.producerExceptionClass);
                break;
            case RETRY_THEN_THROW:
                if (backoff() && autoResolutionEnabled)
                    retryBackendClient();
                else {
                    if (autoResolutionEnabled) {
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                null, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_FAILURE + "." + exception.getClass().getName(),
                                pscConfigurationInternal
                        );
                    }
                    handleException(exception, topicUrisOrPartitions, emitMetrics, followupAction.producerExceptionClass);
                }
                break;
            case RESET_THEN_THROW:
                if (backoff() && autoResolutionEnabled)
                    resetBackendClient();
                else {
                    if (autoResolutionEnabled) {
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                null, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_FAILURE + "." + exception.getClass().getName(),
                                pscConfigurationInternal
                        );
                    }
                    handleException(exception, topicUrisOrPartitions, emitMetrics, followupAction.producerExceptionClass);
                }
                break;
            case NONE:
                retries = autoResolutionRetryCount + 1;
                break;
        }
    }

    /**
     * Performs common actions for resetting a backend producer
     * @throws ProducerException if reset fails
     */
    @Override
    protected void resetBackendClient() throws ProducerException {
        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                null, PscMetrics.PSC_PRODUCER_RESETS_METRIC, pscConfigurationInternal
        );
    }

    /**
     * Performs common actions for retrying a backend producer API call
     */
    @Override
    protected void retryBackendClient() {
        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                null, PscMetrics.PSC_PRODUCER_RETRIES_METRIC, pscConfigurationInternal
        );
    }

    protected void executeBackendCallWithRetries(Runnable backendCaller) throws ProducerException {
        executeBackendCallWithRetries(backendCaller, null);
    }

    protected void executeBackendCallWithRetries(Runnable backendCaller, Set activeTopicUrisOrPartitions) throws ProducerException {
        try {
            backendCaller.run();
        } catch (Exception exception) {
            handleException(exception, activeTopicUrisOrPartitions, true);
            if (autoResolutionEnabled) {
                while (retries <= autoResolutionRetryCount) {
                    try {
                        backendCaller.run();
                        retries = autoResolutionRetryCount + 1;
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                null, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + exception.getClass().getName(), pscConfigurationInternal
                        );
                    } catch (Exception exception2) {
                        handleException(exception2, activeTopicUrisOrPartitions, true);
                    }
                }
            }
        }
    }

    protected <T> T executeBackendCallWithRetriesAndReturn(Callable<T> backendCaller, Set activeTopicUrisOrPartitions) throws ProducerException {
        try {
            return backendCaller.call();
        } catch (Exception exception) {
            handleException(exception, activeTopicUrisOrPartitions, true);
            if (autoResolutionEnabled) {
                while (retries <= autoResolutionRetryCount) {
                    try {
                        T result = backendCaller.call();
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                null, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + exception.getClass().getName(), pscConfigurationInternal
                        );
                        return result;
                    } catch (Exception exception2) {
                        handleException(exception2, activeTopicUrisOrPartitions, true);
                    }
                }
            }
        }

        return null;
    }
}
