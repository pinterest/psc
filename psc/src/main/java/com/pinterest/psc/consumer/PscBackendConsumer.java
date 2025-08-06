package com.pinterest.psc.consumer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscBackendClient;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaErrors;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.exception.consumer.BackendConsumerException;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.interceptor.ConsumerInterceptors;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Objects of this class type are intended to act as mediators between {@link PscConsumer} and backend client library
 * consumers (e.g. {@link org.apache.kafka.clients.consumer.KafkaConsumer}. They are responsible to convert their API
 * calls to proper API calls that are supported by their backend client library. PSC consumer creates one object of
 * this class per backend type and backend cluster pair.
 *
 * @param <K> the key type of PSC messages as configured by the user.
 * @param <V> the value type of PSC messages as configured by the user.
 */
public abstract class PscBackendConsumer<K, V> extends PscBackendClient<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(PscBackendConsumer.class);
    protected PscConfigurationInternal pscConfigurationInternal;
    protected ConsumerInterceptors<K, V> consumerInterceptors;
    protected Class lastExceptionClass = null;

    /**
     * Performs common initializes across all backend consumer types, and delegates backend-specific initializations
     * to {@link #initializeBackend(ServiceDiscoveryConfig, TopicUri)}.
     *
     * @param pscConfigurationInternal the PSC consumer configuration that should be converted to backend specific consumer
     *                                 configuration using
     *                                 {@link com.pinterest.psc.config.PscConsumerToBackendConsumerConfigConverter}.
     * @param discoveryConfig          the service discovery configuration is used to configure the endpoint for the backend
     *                                 consumer.
     * @param topicUri                 the topicUri the consumer is subscribing to. Since a PSC consumer instance can launch multiple
     *                                 backend consumers the client id configured by the user needs to create unique client ids to avoid
     *                                 name clashes with metrics by the backend consumer. For this, PSC appends the backend type and
     *                                 cluster to the provided client id.
     * @throws ConsumerException if the backend consumer instantiation fails.
     */
    public final void initialize(
            PscConfigurationInternal pscConfigurationInternal, ServiceDiscoveryConfig discoveryConfig, TopicUri topicUri
    ) throws ConsumerException {
        this.pscConfigurationInternal = pscConfigurationInternal;
        this.autoResolutionEnabled = pscConfigurationInternal.isAutoResolutionEnabled();
        this.autoResolutionRetryCount = pscConfigurationInternal.getAutoResolutionRetryCount();
        initializeBackend(discoveryConfig, topicUri);
        scheduler.scheduleAtFixedRate(
                this::reportConsumerMetrics,
                new Random().nextInt(pscConfigurationInternal.getConfiguration().getInt(PscConfiguration.PSC_METRICS_FREQUENCY_MS)),
                pscConfigurationInternal.getConfiguration().getInt(PscConfiguration.PSC_METRICS_FREQUENCY_MS),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Specific configuration and instantiation of the backend consumer should be done in this method.
     *
     * @param discoveryConfig the service discovery configuration is used to configure the endpoint for the backend
     *                        consumer.
     * @param topicUri        the topicUri the consumer is subscribing to. Since a PSC consumer instance can launch multiple
     *                        backend consumers the client id configured by the user needs to create unique client ids to avoid
     *                        name clashes with metrics by the backend consumer. For this, PSC appends the backend type and
     *                        cluster to the provided client id.
     * @throws ConsumerException if the backend consumer instantiation fails.
     */
    public abstract void initializeBackend(
            ServiceDiscoveryConfig discoveryConfig, TopicUri topicUri
    ) throws ConsumerException;

    /**
     * Handles reporting metrics from the backend consumer. This method should be overloaded by specific backend clients
     * to implement proper metrics reporting.
     */
    protected synchronized void reportConsumerMetrics() {
    }

    /**
     * Subscribes the backend consumer to the given set of topic URIs. The backend consumer implementation converts the
     * given URIs to the proper format that is supported by the backend client library.
     *
     * @param topicUris set of topic URIs to subscribe to.
     * @throws ConsumerException if subscribing fails due to validation issues, or backend failures.
     */
    public abstract void subscribe(Set<TopicUri> topicUris) throws ConsumerException;

    /**
     * Unsubscribes the backend consumer from any previously subscribed topic URI to stop the consumer consuming from
     * them.
     *
     * @throws ConsumerException if unsubscribe fails due to validation issues, or backend failures.
     */
    public abstract void unsubscribe() throws ConsumerException;

    /**
     * Returns a set of topic URIs that this backend consumer is subscribed to.
     *
     * @return a set of topic URIs.
     * @throws ConsumerException if there is a validation issue.
     */
    public abstract Set<TopicUri> subscription() throws ConsumerException;

    /**
     * Manually assigns the given set of topic URI partitions to this backend consumer. The backend consumer
     * implementation converts the given URIs to the proper format that is supported by the backend client library.
     *
     * @param topicUriPartitions the set of topic URI partitions to assign to the backend consumer.
     * @throws ConsumerException if assignment fails due to validation issues, or backend failures.
     */
    public abstract void assign(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException;

    /**
     * Removes all previously assigned topic URI partitions from this backend consumer's assignment to stop the consumer
     * consuming from them.
     *
     * @throws ConsumerException if unassign fails due to validation issues, or backend failures.
     */
    public abstract void unassign() throws ConsumerException;

    /**
     * Returns a set of topic URI partitions that are assigned to this backend consumer.
     *
     * @return a set of topic URI partitions.
     * @throws ConsumerException if there is a validation issue.
     */
    public abstract Set<TopicUriPartition> assignment() throws ConsumerException;

    /**
     * Polls the consumer for messages from the topic URI the consumer is subscribed to or the topic URI partitions it
     * is assigned.
     *
     * @param pollTimeout the maximum amount of time to wait for the poll to return results.
     * @return an iterator on the messages returned from backend converted to {@link PscConsumerMessage} type.
     * @throws ConsumerException if there are validation issues or backend failures.
     * @throws WakeupException   if there was a prior {@link PscConsumer#wakeup()} call not actioned on by this backend
     *                           consumer yet.
     */
    public abstract PscConsumerPollMessageIterator<K, V> poll(Duration pollTimeout) throws ConsumerException;

    /**
     * Moves the consumption pointer of this consumer to the coordinates of the given message id. The next message to
     * consume would be the one after the one corresponding to the given message id.
     *
     * @param messageId the message id to seek to.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    protected abstract void seek(MessageId messageId) throws ConsumerException;

    /**
     * Moves the consumption pointers of this consumer to the coordinates of the given message ids. {@link PscConsumer}
     * makes sure message ids in this set do not conflict. The next consumed message would be the one after the message
     * identified by the given message ids.
     *
     * @param messageIds the message ids to seek to.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void seek(Set<MessageId> messageIds) throws ConsumerException;

    /**
     * Moves the consumption pointer of this consumer on the given topic URI to the provided timestamp.
     *
     * @param topicUri  the topic URI on which the consumption pointer should be moved.
     * @param timestamp the timestamp (of messages) to which the pointer should point to.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void seekToTimestamp(TopicUri topicUri, long timestamp) throws ConsumerException;

    /**
     * Moves the consumption pointer of this consumer on the given topic URI partitions to the provided timestamps.
     *
     * @param seekPositions a map of topic URI partition to timestamp.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void seekToTimestamp(Map<TopicUriPartition, Long> seekPositions) throws ConsumerException;

    /**
     * Moves the consumption pointer of this consumer on the given topic URI partitions to the provided offsets.
     *
     * @param seekPositions a map of topic URI partition to offset.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void seekToOffset(Map<TopicUriPartition, Long> seekPositions) throws ConsumerException;

    /**
     * Moves the consumer pointer to the beginning of given topic URI partitions.
     *
     * @param topicUriPartitions the set of topic URI partitions for which the consumer pointer should seek to
     *                           beginning.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void seekToBeginning(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException;

    /**
     * Moves the consumer pointer to the end of given topic URI partitions.
     *
     * @param topicUriPartitions the set of topic URI partitions for which the consumer pointer should seek to end.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void seekToEnd(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException;

    /**
     * Suspend fetching from the specified partitions. Future calls to {@link #poll(Duration)} will not return any
     * records from these partitions until they are resumed using {@link #resume(Collection)}. Note that this method
     * does not affect partition subscription.
     *
     * @param topicUriPartitions the set of topic URI partitions to pause fetching from.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void pause(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException;

    /**
     * Resume fetching from the specified partitions. Future calls to {@link #poll(Duration)} will return records from
     * these partitions if there are any to return. If the consumer was not previously paused, this method has no
     * effect.
     *
     * @param topicUriPartitions the set of topic URI partitions to resume fetching from.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void resume(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException;

    /**
     * Commits the consumption coordinate of the consumer for the given message ids. This is a synchronous commit.
     *
     * @return a set of message ids that represent the latest committed offsets for underlying topic uri partition.
     * @throws ConsumerException if there are validation issues or backend failures.
     * @throws WakeupException   if there was a prior {@link PscConsumer#wakeup()} call not actioned on by this backend
     *                           consumer yet.
     */
    public abstract Set<MessageId> commitSync() throws ConsumerException, WakeupException;

    /**
     * Commits the consumption coordinate of the consumer for the given message ids. This is a synchronous commit.
     *
     * @param messageIds message ids to commit.
     * @throws ConsumerException if there are validation issues or backend failures.
     * @throws WakeupException   if there was a prior {@link PscConsumer#wakeup()} call not actioned on by this backend
     *                           consumer yet.
     */
    public abstract void commitSync(Set<MessageId> messageIds) throws ConsumerException, WakeupException;

    /**
     * Commits the current consumption coordinates of the consumer asynchronously, and triggers the callback function
     * once complete.
     *
     * @param offsetCommitCallback the function to execute once asynchronous commit is complete.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void commitAsync(OffsetCommitCallback offsetCommitCallback) throws ConsumerException;

    /**
     * Commits the current consumption coordinate of the consumer for the given message ids asynchronously, and triggers
     * the callback function once complete.
     *
     * @param messageIds           message ids to commit.
     * @param offsetCommitCallback the function to execute once asynchronous commit is complete.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract void commitAsync(Set<MessageId> messageIds, OffsetCommitCallback offsetCommitCallback) throws ConsumerException;

    /**
     * Returns the set of topic URI partitions associated with the given topic URI.
     *
     * @param topicUri the topic URI for which all topic URI partitions to be returned.
     * @return a set of topic URI partitions.
     * @throws ConsumerException if there are validation issues or backend failures.
     * @throws WakeupException   if there was a prior {@link PscConsumer#wakeup()} call not actioned on by this backend
     *                           consumer yet.
     */
    public abstract Set<TopicUriPartition> getPartitions(TopicUri topicUri) throws ConsumerException, WakeupException;

    /**
     * Returns a set of topic names in cluster for consumer adhoc use
     *
     * @return a set of topic names
     * @throws ConsumerException
     */
    public abstract Set<String> getTopicNames() throws ConsumerException;

    /**
     * Wakes up the consumer to interrupt ongoing execution of APIs, when applicable.
     */
    public abstract void wakeup();

    /**
     * Returns the message id associated with the last committed offset of the given topic URI partition.
     *
     * @param topicUriPartition a topic URI partition.
     * @return a message id that corresponds to last committed offset.
     * @throws ConsumerException if there are validation issues or backend failures.
     * @throws WakeupException   if there was a prior {@link PscConsumer#wakeup()} call not actioned on by this backend
     *                           consumer yet.
     */
    public abstract MessageId committed(TopicUriPartition topicUriPartition) throws ConsumerException, WakeupException;

    /**
     * Returns a collection of MessageIds associated with the last committed offset of the given topic URI partitions.
     *
     * @param topicUriPartitions collection of topic URI partition.
     * @return MessageIds that corresponds to last committed offset.
     * @throws ConsumerException if there are validation issues or backend failures.
     * @throws WakeupException   if there was a prior {@link PscConsumer#wakeup()} call not actioned on by this backend
     *                           consumer yet.
     */
    public abstract Collection<MessageId> committed(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException, WakeupException;
    /**
     * Returns the start offset for each of the given topic URI partitions.
     *
     * @param topicUriPartitions a set of topic URI partition.
     * @return a map of topic URI partition to start offset.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract Map<TopicUriPartition, Long> startOffsets(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException;

    /**
     * Returns the last offset for each of the given topic URI partitions.
     *
     * @param topicUriPartitions a set of topic URI partition.
     * @return a map of topic URI partition to last offset.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract Map<TopicUriPartition, Long> endOffsets(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException;

    /**
     * Returns the consumption position of the backend consumer on the given topic URI partition.
     *
     * @param topicUriPartition a topic URI partition.
     * @return the next offset to be consumed by this backend consumer.
     * @throws ConsumerException if there are validation issues or backend failures.
     * @throws WakeupException   if there was a prior {@link PscConsumer#wakeup()} call not actioned on by this backend
     *                           consumer yet.
     */
    public abstract long position(TopicUriPartition topicUriPartition) throws ConsumerException, WakeupException;

    /**
     * Finds the corresponding message id of a timestamp for each of the given topic URI partitions.
     *
     * @param timestampByTopicUriPartition a map of topic URI partition to timestamp.
     * @return a map of topic URI partition to message id.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public abstract Map<TopicUriPartition, MessageId> getMessageIdByTimestamp(Map<TopicUriPartition, Long> timestampByTopicUriPartition) throws ConsumerException;

    /**
     * Closes the backend consumer.
     *
     * @param timeout the timeout by which the backend consumer is expected to close.
     * @throws ConsumerException if there are validation issues or backend failures.
     */
    public void close(Duration timeout) throws ConsumerException {
        scheduler.shutdown();
    }

    /**
     * Sets the consumer interceptors (both default and user defined) only when creating the consumer.
     *
     * @param consumerInterceptors the interceptors to assign to this consumer.
     */
    public final void setConsumerInterceptors(ConsumerInterceptors<K, V> consumerInterceptors) {
        this.consumerInterceptors = consumerInterceptors;
    }

    /**
     * Returns the interceptors for this backend consumer. If none is configured, a default object is
     * created and returned.
     *
     * @return the configured interceptors for this backend consumer.
     */
    protected final ConsumerInterceptors<K, V> getConsumerInterceptors() {
        if (consumerInterceptors == null) {
            // default case
            consumerInterceptors = new ConsumerInterceptors<K, V>(
                    null,
                    pscConfigurationInternal,
                    pscConfigurationInternal.getPscConsumerKeyDeserializer(),
                    pscConfigurationInternal.getPscConsumerValueDeserializer()
            );
        }
        return consumerInterceptors;
    }

    /**
     * Performs preparations for Throwing the proper PSC consumer exception based on the exception coming from the
     * backend client.
     *
     * @param backendException       the exception from the backend client
     * @param topicUrisOrPartitions  involved topic URIs or topic URI partitions
     * @param emitMetrics            whether to emit metrics
     * @param consumerExceptionClass the PSC consumer exception class to throw
     * @throws ConsumerException the superclass of the to-be-thrown exception
     */
    protected void handleException(Exception backendException, Set topicUrisOrPartitions, boolean emitMetrics, Class<? extends ConsumerException> consumerExceptionClass) throws ConsumerException {
        PscErrorHandler.handle(
                new BackendConsumerException(backendException, PscUtils.BACKEND_TYPE_KAFKA),
                topicUrisOrPartitions,
                emitMetrics,
                pscConfigurationInternal
        );
        retries = 0;

        try {
            throw consumerExceptionClass.getConstructor(Throwable.class).newInstance(backendException);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new ConsumerException(
                    String.format(
                            "Failed to instantiate from the exception class '%s', throwing the generic ConsumerException instead.",
                            consumerExceptionClass.getName()
                    ),
                    backendException
            );
        }
    }

    /**
     * Depending on the thrown exception from the backend client, it decides about the actions to take.
     *
     * @param exception             the exception from the backend client
     * @param topicUrisOrPartitions involved topic URIs or topic URI partitions
     * @param emitMetrics           whether to emit metrics
     * @throws ConsumerException the superclass of the to-be-thrown exception
     */
    protected void handleException(Exception exception, Set topicUrisOrPartitions, boolean emitMetrics) throws ConsumerException {
        PscErrorHandler.ConsumerAction followupAction = KafkaErrors.shouldHandleConsumerException(exception, autoResolutionEnabled);
        handleExceptionLogging(exception, followupAction);

        if (lastExceptionClass != exception.getClass()) {
            lastExceptionClass = exception.getClass();
            retries = 0;
        }

        switch (followupAction.actionType) {
            case THROW:
                handleException(exception, topicUrisOrPartitions, emitMetrics, followupAction.consumerExceptionClass);
                break;
            case RETRY_THEN_THROW:
                if (backoff() && autoResolutionEnabled) {
                    retryBackendClient();
                    if (retries > 1) {
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                null, PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_FAILURE + "." + exception.getClass().getName(), pscConfigurationInternal
                        );
                    }
                } else {
                    if (autoResolutionEnabled) {
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                null, PscMetrics.PSC_CONSUMER_RETRIES_REACHED_LIMIT_METRIC, pscConfigurationInternal
                        );
                    }
                    handleException(exception, topicUrisOrPartitions, emitMetrics, followupAction.consumerExceptionClass);
                }
                break;
            case RESET_THEN_THROW:
                if (backoff() && autoResolutionEnabled)
                    resetBackendClient();
                else {
                    if (autoResolutionEnabled) {
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                null, PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_FAILURE + "." + exception.getClass().getName(), pscConfigurationInternal
                        );
                    }
                    handleException(exception, topicUrisOrPartitions, emitMetrics, followupAction.consumerExceptionClass);
                }
                break;
            case NONE:
                retries = autoResolutionRetryCount + 1;
                break;
        }
    }

    private void handleExceptionLogging(Exception exception, PscErrorHandler.ConsumerAction followupAction) {
        if (exception.getClass().equals(WakeupException.class) || exception.getClass().equals(org.apache.kafka.common.errors.WakeupException.class)) {
            // debug level for WakeupException
            logger.debug("Exception {} caught from the backend consumer:", exception.getClass().getName(), exception);
            logger.debug("Auto resolution status: {} (remaining retries: {})", autoResolutionEnabled, autoResolutionRetryCount - retries);
            logger.debug("Follow up action to this exception: {}", followupAction.actionType);
        } else {
            logger.warn("Exception {} caught from the backend consumer:", exception.getClass().getName(), exception);
            logger.warn("Auto resolution status: {} (remaining retries: {})", autoResolutionEnabled, autoResolutionRetryCount - retries);
            logger.warn("Follow up action to this exception: {}", followupAction.actionType);
        }
    }

    /**
     * Performs common actions for resetting a backend consumer
     *
     * @throws ConsumerException if reset fails
     */
    @Override
    protected void resetBackendClient() throws ConsumerException {
        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                null, PscMetrics.PSC_CONSUMER_RESETS_METRIC, pscConfigurationInternal
        );
    }

    /**
     * Performs common actions for retrying a backend consumer API call
     */
    @Override
    protected void retryBackendClient() {
        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                null, PscMetrics.PSC_CONSUMER_RETRIES_METRIC, pscConfigurationInternal
        );
    }

    protected void executeBackendCallWithRetries(Runnable backendCaller) throws ConsumerException {
        executeBackendCallWithRetries(backendCaller, null);
    }

    protected void executeBackendCallWithRetries(Runnable backendCaller, Set activeTopicUrisOrPartitions) throws ConsumerException {
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
                                null, PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + exception.getClass().getName(), pscConfigurationInternal
                        );
                    } catch (Exception exception2) {
                        handleException(exception2, activeTopicUrisOrPartitions, true);
                    }
                }
            }
        }
    }

    protected <T> T executeBackendCallWithRetriesAndReturn(Callable<T> backendCaller) throws ConsumerException {
        return executeBackendCallWithRetriesAndReturn(backendCaller, null);
    }

    protected <T> T executeBackendCallWithRetriesAndReturn(Callable<T> backendCaller, Set activeTopicUrisOrPartitions) throws ConsumerException {
        try {
            return backendCaller.call();
        } catch (Exception exception) {
            handleException(exception, activeTopicUrisOrPartitions, true);
            if (autoResolutionEnabled) {
                while (retries <= autoResolutionRetryCount) {
                    try {
                        T result = backendCaller.call();
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                null, PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + exception.getClass().getName(), pscConfigurationInternal
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
