package com.pinterest.psc.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.event.PscEvent;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.creation.PscBackendConsumerCreator;
import com.pinterest.psc.consumer.creation.PscConsumerCreatorManager;
import com.pinterest.psc.consumer.listener.MessageListener;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.PscStartupException;
import com.pinterest.psc.interceptor.ConsumerInterceptors;
import com.pinterest.psc.interceptor.Interceptors;
import com.pinterest.psc.interceptor.TypePreservingInterceptor;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.serde.Deserializer;
import org.apache.commons.configuration2.Configuration;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PscConsumer<K, V> implements AutoCloseable {
    private static final PscLogger logger = PscLogger.getLogger(PscConsumer.class);

    static {
        logger.addContext("pscPid", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        logger.addContext("pscTid", String.valueOf(Thread.currentThread().getId()));
        logger.addContext("pscHost", PscCommon.getHostname());
    }

    private static final long NO_CURRENT_THREAD = -1L;
    private ExecutorService listenerExecutor;

    private Environment environment;
    private PscConsumerCreatorManager creatorManager;
    private MessageListener<K, V> messageListener;
    private Interceptors<K, V> interceptors;
    private ConsumerInterceptors<K, V> consumerInterceptors;

    // flags
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean subscribed = new AtomicBoolean(false);
    private final AtomicBoolean assigned = new AtomicBoolean(false);
    private final AtomicBoolean listening = new AtomicBoolean(false);
    private final AtomicInteger wakeups = new AtomicInteger(-1);

    // This should keep the up-to-date URI to consumer mapping between subscribe/unsubscribe calls
    private final Map<TopicUri, PscBackendConsumer<K, V>> subscriptionMap = new ConcurrentHashMap<>();
    // This should keep the up-to-date URI to consumer mapping between assign calls
    private final Map<TopicUriPartition, PscBackendConsumer<K, V>> assignmentMap = new ConcurrentHashMap<>();
    // This is a translation map from a topic URI (string format) to parsed uris
    private final Map<String, TopicUri> topicUriStrToTopicUri = new HashMap<>();
    // This is the source of truth of the currently registered consumers
    private Set<PscBackendConsumer<K, V>> backendConsumers = new HashSet<>();

    private PscMetricRegistryManager pscMetricRegistryManager;

    private final PscConfigurationInternal pscConfigurationInternal;

    // currentThread holds the threadId of the current thread accessing PscConsumer and is used to prevent
    // multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow re-entrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);
    private ConsumerRebalanceListener rebalanceListener;

    /**
     * Creates a PscConsumer object using the configuration in the provided config file to override the default
     * PscConsumer configurations.
     *
     * @param customPscConfigurationFilePath the file containing overridden configuration in key=value lines.
     * @throws ConfigurationException in case there are validation errors with the configuration.
     * @throws ConsumerException      in case there are errors when initializing the PscConsumer instance.
     */
    public PscConsumer(String customPscConfigurationFilePath) throws ConfigurationException, ConsumerException {
        if (customPscConfigurationFilePath == null)
            throw new ConsumerException("Null parameter was passed to API PscConsumer(String)");
        pscConfigurationInternal = new PscConfigurationInternal(customPscConfigurationFilePath, PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
        initialize();
    }

    /**
     * Creates a PscConsumer object using the configuration in the provided configuration object to override the default
     * PscConsumer configurations.
     * @deprecated
     * Please use <code>PscConsumer(PscConfiguration)</code> instead.
     *
     * @param configuration the config object that contains all overridden configurations.
     * @throws ConfigurationException in case there are validation errors with the configuration.
     * @throws ConsumerException      in case there are errors when initializing the PscConsumer instance.
     */
    @Deprecated
    public PscConsumer(Configuration configuration) throws ConfigurationException, ConsumerException {
        if (configuration == null)
            throw new ConsumerException("Null parameter was passed to API PscConsumer(Configuration)");
        pscConfigurationInternal = new PscConfigurationInternal(configuration, PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
        initialize();
    }

    /**
     * Creates a PscConsumer object using the configuration in the provided PSC configuration object to override the
     * default PscConsumer configurations.
     *
     * @param pscConfiguration the PSC configuration object that contains all overridden configurations.
     * @throws ConfigurationException in case there are validation errors with the configuration.
     * @throws ConsumerException      in case there are errors when initializing the PscConsumer instance.
     */
    public PscConsumer(PscConfiguration pscConfiguration) throws ConfigurationException, ConsumerException {
        if (pscConfiguration == null)
            throw new ConsumerException("Null parameter was passed to API PscConsumer(PscConfiguration)");
        pscConfigurationInternal = new PscConfigurationInternal(pscConfiguration, PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
        initialize();
    }

    /**
     * Returns the created listener instance, in case the PscConsumer is configured to use one. This API is thread-safe.
     *
     * @return the listener instance based on PscConsumer configuration; or null if not configured
     */
    public MessageListener<K, V> getListener() {
        return messageListener;
    }

    /**
     * Returns the list of typed (post-deserialization) interceptor objects. These objects are either passed in the
     * initial configuration or they are instantiated by PSC consumer based on the configured FQDN.
     *
     * @return list of typed consumer interceptor objects.
     */
    public List<TypePreservingInterceptor<K, V>> getTypedInterceptors() {
        return interceptors.getTypedDataInterceptors();
    }

    /**
     * Returns the list of raw (pre-deserialization) interceptor objects. These objects are either passed in the
     * initial configuration or they are instantiated by PSC consumer based on the configured FQDN.
     *
     * @return list of raw consumer interceptor objects.
     */
    public List<TypePreservingInterceptor<byte[], byte[]>> getRawInterceptors() {
        return interceptors.getRawDataInterceptors();
    }

    @VisibleForTesting
    protected Interceptors<K, V> getInterceptors() {
        return interceptors;
    }

    @SuppressWarnings("unchecked")
    private void initialize() {
        Deserializer<K> keyDeserializer = pscConfigurationInternal.getPscConsumerKeyDeserializer();
        Deserializer<V> valueDeserializer = pscConfigurationInternal.getPscConsumerValueDeserializer();

        List<TypePreservingInterceptor<byte[], byte[]>> rawInterceptors = pscConfigurationInternal.getRawPscConsumerInterceptors();
        List<TypePreservingInterceptor<K, V>> typedInterceptors = pscConfigurationInternal.getTypedPscConsumerInterceptors()
                .stream().map(interceptor -> (TypePreservingInterceptor<K, V>) interceptor).collect(Collectors.toList());

        interceptors = new Interceptors<>(rawInterceptors, typedInterceptors, pscConfigurationInternal);
        consumerInterceptors = new ConsumerInterceptors<>(interceptors, pscConfigurationInternal, keyDeserializer, valueDeserializer);
        messageListener = pscConfigurationInternal.getPscConsumerMessageListener();

        creatorManager = new PscConsumerCreatorManager();
        environment = pscConfigurationInternal.getEnvironment();

        PscMetricRegistryManager.getInstance().initialize(pscConfigurationInternal);
        PscMetricRegistryManager.getInstance().enableJvmMetrics("_jvm", pscConfigurationInternal);

        if (messageListener != null) {
            listenerExecutor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("psc-listener-thread-%d").build()
            );
        }

        getPscMetricRegistryManager().incrementCounterMetric(null, PscMetrics.PSC_CONSUMER_COUNT, pscConfigurationInternal);
    }

    @VisibleForTesting
    protected Collection<PscBackendConsumer<K, V>> getBackendConsumers() {
        return backendConsumers;
    }

    /**
     * This method can be internally used by <code>PscConsumer</code> in situations where the API call on the backend
     * consumer throws exceptions that are not usually temporary and can only be fixed by closing the backend consumer
     * and starting another one with similar properties.
     *
     * @param pscBackendConsumer
     * @throws ConsumerException
     * @throws ConfigurationException
     */
    @VisibleForTesting
    protected void reset(PscBackendConsumer<K, V> pscBackendConsumer) throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            Set<TopicUriPartition> assignment = new HashSet<>(pscBackendConsumer.assignment());

            // first properly terminate and close the backend consumer
            pscBackendConsumer.wakeup();
            if (subscribed.get()) {
                pscBackendConsumer.unsubscribe();
                pscBackendConsumer.subscription().forEach(subscriptionMap::remove);
            } else if (assigned.get()) {
                pscBackendConsumer.unassign();
                pscBackendConsumer.assignment().forEach(assignmentMap::remove);
            }

            try {
                pscBackendConsumer.close();
            } catch (Exception exception) {
                throw new ConsumerException(String.format("Backend consumer failed to close.\n%s", exception));
            }

            backendConsumers.remove(pscBackendConsumer);

            // then remove it from the backend consumer creator map
            Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();
            Map<String, Set<TopicUriPartition>> backendToTopicUriPartitions = new HashMap<>();

            for (TopicUriPartition topicUriPartition : assignment) {
                TopicUri topicUri = topicUriPartition.getTopicUri();
                backendToTopicUriPartitions.computeIfAbsent(topicUri.getBackend(), b -> new HashSet<>()).add(topicUriPartition);
            }

            for (Map.Entry<String, Set<TopicUriPartition>> entry : backendToTopicUriPartitions.entrySet()) {
                if (creator.containsKey(entry.getKey())) {
                    creator.get(entry.getKey()).dismissBackendConsumer(entry.getValue(), pscBackendConsumer);
                } else {
                    throw new ConsumerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(entry.getKey()));
                }
            }

            // finally attempt at relaunching a similar backend consumer
            if (subscribed.get()) {
                subscribe(subscription());
            } else if (assigned.get()) {
                assign(assignment);
            }
        } finally {
            release();
        }
    }

    /**
     * Assigns the provided {@link TopicUriPartition}s to this PscConsumer instance. This API is not thread-safe.
     *
     * @param topicUriPartitions the collection of {@link TopicUriPartition}s
     * @throws ConsumerException      if the consumer is already subscribed to some URIs, or if there is an invalid
     *                                TopicUriPartition in the provided collection, or if any unexpected error occurs.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     *                                *
     */
    @SuppressWarnings("unchecked")
    public void assign(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            topicUriPartitions = validateTopicUriPartitions(topicUriPartitions);

            if (subscribed.get()) {
                throw new ConsumerException("[PSC] Calling assign() while consumer was assigned partitions via " +
                        "subscribe() api.");
            }

            // unassign if empty
            if (topicUriPartitions.isEmpty()) {
                unassign();
                return;
            }

            // dispatch topicUriPartitions to creators based on the backend
            @SuppressWarnings("rawtypes")
            Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();
            Map<String, Set<TopicUriPartition>> backendToTopicUriPartitions = new HashMap<>();

            for (TopicUriPartition topicUriPartition : topicUriPartitions) {
                TopicUri topicUri = topicUriPartition.getTopicUri();
                backendToTopicUriPartitions.computeIfAbsent(topicUri.getBackend(), b -> new HashSet<>()).add(topicUriPartition);
            }

            Set<PscBackendConsumer<K, V>> newBackendConsumers = new HashSet<>();
            for (Map.Entry<String, Set<TopicUriPartition>> entry : backendToTopicUriPartitions.entrySet()) {
                if (creator.containsKey(entry.getKey())) {
                    Set<PscBackendConsumer<K, V>> activeConsumers = creator.get(entry.getKey()).getAssignmentConsumers(
                            environment,
                            pscConfigurationInternal,
                            consumerInterceptors,
                            entry.getValue(),
                            wakeups.get() >= 0,
                            true
                    );

                    // update the consumer list with the new consumers that were created
                    newBackendConsumers.addAll(activeConsumers);
                } else {
                    throw new ConsumerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(entry.getKey()));
                }
            }

            updateAssignmentsAndConsumers(newBackendConsumers);

            // set assigned to true
            boolean prevAssigned = assigned.getAndSet(true);

            // Only initialize the listener thread if needed.
            // If subscribed was already true, the previous loop would still be active and no need to resubmit.
            if (!prevAssigned && messageListener != null)
                listenerExecutor.execute(() -> listenerPoll(assigned));
        } finally {
            release();
        }
    }

    private void updateAssignmentsAndConsumers(Set<PscBackendConsumer<K, V>> newBackendConsumers) throws ConsumerException {
        // cleanup and recollect the assignment map of each consumer
        assignmentMap.clear();

        // only add consumers that are active
        for (PscBackendConsumer<K, V> consumer : newBackendConsumers) {
            Set<TopicUriPartition> topicUriPartitions = consumer.assignment();
            for (TopicUriPartition topicUriPartition : topicUriPartitions)
                assignmentMap.put(topicUriPartition, consumer);
        }

        Set<PscBackendConsumer<K, V>> unusedConsumers = Sets.difference(backendConsumers, newBackendConsumers);

        backendConsumers = newBackendConsumers;
        // remove all consumers that are inactive after the assign
        for (PscBackendConsumer<K, V> unusedConsumer : unusedConsumers) {
            unusedConsumer.unassign();
            try {
                unusedConsumer.close();
            } catch (Exception e) {
                throw new ConsumerException(e);
            }
        }
    }

    /**
     * @return a set of assigned {@link TopicUriPartition}s to this PscConsumer instance. This API is not thread-safe.
     * @throws ConsumerException if there is a validation failure or collecting metric from backend consumers fails.
     */
    public Set<TopicUriPartition> assignment() throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            Set<TopicUriPartition> assignment = new HashSet<>();
            for (PscBackendConsumer<K, V> backendConsumer : backendConsumers) {
                try {
                    assignment.addAll(backendConsumer.assignment());
                } catch (ConsumerException e) {
                    logger.warn("Could not extract assignment of the backend consumer {}", backendConsumer.toString(), e);
                }
            }
            return assignment;
        } finally {
            release();
        }
    }

    private void unassign(Duration timeout) throws ConsumerException {
        acquireAndEnsureOpen();
        internalUnassign(Optional.of(timeout));
    }

    /**
     * Removes all partitions that are manually assigned to this PscConsumer using {@link #assign(Collection)}. If
     * there is a listener configured, there may be a slight delay until the listener poll loop detects this
     * un-assignment.
     *
     * @throws ConsumerException if unassign or close call on backend consumers throw an exception.
     */
    public void unassign() throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            internalUnassign(Optional.empty());
        } finally {
            release();
        }
    }

    private void waitForListenerToComplete() {
        while (messageListener != null && listening.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.warn("", e);
            }
        }
    }

    /**
     * Subscribes this consumer to the provided topic URIs. This API is not thread-safe.
     *
     * @param topicUrisAsString the collection of topic URIs
     * @throws ConsumerException      if the consumer is actively using the {@link #assign(Collection)} api, or if
     *                                there is an invalid topic URI in the provided collection, or if any unexpected error occurs.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    @SuppressWarnings("unchecked")
    public void subscribe(Collection<String> topicUrisAsString, ConsumerRebalanceListener consumerRebalanceListener) throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        this.rebalanceListener = consumerRebalanceListener;
        try {
            Set<TopicUri> topicUris = validateTopicUris(topicUrisAsString);

            if (assigned.get()) {
                throw new ConsumerException("[PSC] Calling subscribe() while consumer was assigned partitions via " +
                        "assign() api.");
            }

            // unsubscribe if empty
            if (topicUrisAsString.isEmpty()) {
                unsubscribe();
                return;
            }

            // dispatch topicUris to creators based on the backend
            @SuppressWarnings("rawtypes")
            Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();
            Map<String, Set<TopicUri>> backendToTopicUris = new HashMap<>();

            for (TopicUri topicUri : topicUris)
                backendToTopicUris.computeIfAbsent(topicUri.getBackend(), b -> new HashSet<>()).add(topicUri);

            Set<PscBackendConsumer<K, V>> newBackendConsumers = new HashSet<>();
            for (Map.Entry<String, Set<TopicUri>> entry : backendToTopicUris.entrySet()) {
                if (creator.containsKey(entry.getKey())) {
                    Set<PscBackendConsumer<K, V>> activeConsumers = creator.get(entry.getKey()).getConsumers(
                            environment,
                            pscConfigurationInternal,
                            consumerInterceptors,
                            entry.getValue(),
                            consumerRebalanceListener,
                            wakeups.get() >= 0,
                            true
                    );

                    // update the consumer list with the new consumers that were created
                    newBackendConsumers.addAll(activeConsumers);
                } else {
                    throw new ConsumerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(entry.getKey()));
                }
            }

            updateSubscriptionsAndConsumers(newBackendConsumers);

            // set subscribed to true
            boolean prevSubscribed = subscribed.getAndSet(true);

            // Only initialize the listener thread if needed.
            // If subscribed was already true, the previous loop would still be active and no need to resubmit.
            if (!prevSubscribed && messageListener != null)
                listenerExecutor.execute(() -> listenerPoll(subscribed));
        } finally {
            release();
        }
    }

    @SuppressWarnings("unchecked")
    public void subscribe(Collection<String> topicUrisAsString)
        throws ConsumerException, ConfigurationException {
        subscribe(topicUrisAsString, null);
    }

    private void updateSubscriptionsAndConsumers(Set<PscBackendConsumer<K, V>> newBackendConsumers) throws ConsumerException {
        // cleanup and recollect the subscription map of each consumer
        topicUriStrToTopicUri.clear();
        subscriptionMap.clear();

        // only add consumers that are active
        for (PscBackendConsumer<K, V> consumer : newBackendConsumers) {
            Set<TopicUri> subscription = consumer.subscription();
            for (TopicUri topicUri : subscription) {
                topicUriStrToTopicUri.put(topicUri.toString(), topicUri);
                subscriptionMap.put(topicUri, consumer);
            }
        }

        Set<PscBackendConsumer<K, V>> unusedConsumers = Sets.difference(backendConsumers, newBackendConsumers);

        backendConsumers = newBackendConsumers;
        // remove all consumers that are inactive after the subscribe
        for (PscBackendConsumer<K, V> unusedConsumer : unusedConsumers) {
            unusedConsumer.unsubscribe();
            try {
                unusedConsumer.close();
            } catch (Exception e) {
                throw new ConsumerException(e);
            }
        }
    }

    private void listenerPoll(AtomicBoolean subscribedOrAssigned) {
        listening.set(true);
        while (!closed.get() && subscribedOrAssigned.get()) {
            PscConsumerPollMessageIterator<K, V> messages;
            try {
                messages = internalPoll(Duration.ofMillis(pscConfigurationInternal.getPscConsumerPollTimeoutMs()));
            } catch (Throwable t) {
                logger.error("Message listener error when fetching messages", t);
                return;
            }
            while (messages.hasNext()) {
                PscConsumerMessage<K, V> message = messages.next();
                try {
                    messageListener.handle(this, message);
                } catch (Throwable t) {
                    logger.error("Message listener failed to process message: {}", message, t);
                }
            }
        }
        listening.set(false);
    }

    /**
     * @return a set of subscribed topic URIs to this PscConsumer instance. This API is not thread-safe.
     * @throws ConsumerException if there is a validation failure.
     */
    public Set<String> subscription() throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            return subscriptionMap.keySet().stream().map(TopicUri::getTopicUriAsString).collect(Collectors.toSet());
        } finally {
            release();
        }
    }

    private void unsubscribe(Duration timeout) throws ConsumerException {
        acquireAndEnsureOpen();
        internalUnsubscribe(Optional.of(timeout));
    }

    /**
     * Removes all topic URIs that this PscConsumer previously subscribed to using {@link #subscribe(Collection)}. If
     * there is a listener configured, there may be a slight delay until the listener poll loop detects this
     * un-subscription.
     *
     * @throws ConsumerException if unsubscribe or close call on backend consumers throw an exception.
     */
    public void unsubscribe() throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            internalUnsubscribe(Optional.empty());
        } finally {
            release();
        }
    }

    private void internalUnsubscribe(Optional<Duration> optionalTimeout) throws ConsumerException {
        subscribed.set(false);
        topicUriStrToTopicUri.clear();
        subscriptionMap.clear();

        // if there is a listener, wait until it stops listening to messages
        waitForListenerToComplete();

        List<Exception> exceptions = new ArrayList<>();
        for (PscBackendConsumer<K, V> consumer : backendConsumers) {
            consumer.unsubscribe();
            try {
                if (optionalTimeout.isPresent())
                    consumer.close(optionalTimeout.get());
                else
                    consumer.close();
            } catch (Exception exception) {
                exceptions.add(exception);
            }
        }

        backendConsumers.clear();
        creatorManager.reset();

        if (!exceptions.isEmpty()) {
            throw new ConsumerException(
                    String.format(
                            "Some backend consumers failed to close.\n%s",
                            exceptions.stream().map(Throwable::getMessage).collect(Collectors.joining("\n"))
                    )
            );
        }
    }

    private void internalUnassign(Optional<Duration> optionalTimeout) throws ConsumerException {
        assigned.set(false);
        assignmentMap.clear();

        // if there is a listener, wait until it stops listening to messages
        waitForListenerToComplete();

        List<Exception> exceptions = new ArrayList<>();
        // since backend consumers are initialized in the assign() method, they are closed in this call.
        for (PscBackendConsumer<K, V> consumer : backendConsumers) {
            consumer.unassign();
            try {
                if (optionalTimeout.isPresent())
                    consumer.close(optionalTimeout.get());
                else
                    consumer.close();
            } catch (Exception exception) {
                exceptions.add(exception);
            }
        }
        backendConsumers.clear();
        creatorManager.reset();

        if (!exceptions.isEmpty()) {
            throw new ConsumerException(
                    String.format(
                            "Some backend consumers failed to close.\n%s",
                            exceptions.stream().map(Throwable::getMessage).collect(Collectors.joining("\n"))
                    )
            );
        }
    }

    private void ensureOpen() throws ConsumerException {
        if (closed.get()) {
            throw new ConsumerException(ExceptionMessage.ALREADY_CLOSED_EXCEPTION);
        }
    }

    private void acquireAndEnsureOpen() throws ConsumerException {
        acquire();
        if (closed.get()) {
            release();
            throw new ConsumerException(ExceptionMessage.ALREADY_CLOSED_EXCEPTION);
        }
    }

    private void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != this.currentThread.get() && !this.currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException(ExceptionMessage.MULTITHREADED_EXCEPTION);
        refcount.incrementAndGet();
        logger.addContext("pscTid", String.valueOf(this.currentThread.get()));
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0)
            this.currentThread.set(NO_CURRENT_THREAD);
    }

    /**
     * Polls all backend consumers this PscConsumer is associated with for messages. It uses the default or customized
     * timout in the configuration and returns an iterator on these messages. This API is not thread-safe.
     *
     * @return an iterator on polled messages from all backend consumers.
     * @throws ConsumerException if an exception from backend consumers bubbles up.
     * @throws WakeupException   if {@link #wakeup()} is called on this PscConsumer.
     */
    public PscConsumerPollMessageIterator<K, V> poll() throws ConsumerException, WakeupException {
        return poll(Duration.ofMillis(pscConfigurationInternal.getPscConsumerPollTimeoutMs()));
    }

    /**
     * Polls all backend consumers this PscConsumer is associated with for messages. It uses the provided poll timeout
     * and returns an iterator on these messages. This API is not thread-safe.
     *
     * @param pollTimeout poll timeout duration which specifies the maximum wait time for poll whether there
     *                    are messages to consume or not.
     * @return an iterator on polled messages from all backend consumers.
     * @throws ConsumerException if an exception from backend consumers bubbles up.
     * @throws WakeupException   if {@link #wakeup()} is called on this PscConsumer.
     */
    public PscConsumerPollMessageIterator<K, V> poll(Duration pollTimeout) throws ConsumerException, WakeupException {
        acquireAndEnsureOpen();
        try {
            if (messageListener != null) {
                throw new ConsumerException(ExceptionMessage.MUTUALLY_EXCLUSIVE_APIS("poll()", "MessageListener"));
            } else if (!subscribed.get() && !assigned.get()) {
                throw new ConsumerException(ExceptionMessage.NO_SUBSCRIPTION_ASSIGNMENT("poll()"));
            }
            return internalPoll(pollTimeout);
        } finally {
            release();
        }
    }

    @VisibleForTesting
    protected PscConsumerPollMessageIterator<K, V> internalPoll(Duration pollTimeout) throws ConsumerException, WakeupException {
        List<PscConsumerPollMessageIterator<K, V>> consumersMessages = new ArrayList<>();
        for (PscBackendConsumer<K, V> backendConsumer : backendConsumers) {
            long startTs = System.currentTimeMillis();
            PscConsumerPollMessageIterator<K, V> messages;
            try {
                messages = backendConsumer.poll(pollTimeout);
            } catch (WakeupException we) {
                if (this.wakeups.incrementAndGet() == backendConsumers.size()) {
                    // when all backend consumers are interrupted, reset wakeups and throw exception
                    this.wakeups.set(-1);
                    PscErrorHandler.handle(we, subscriptionMap.keySet(), true, pscConfigurationInternal);
                    throw new WakeupException(we);
                } else
                    continue;
            }
            long stopTs = System.currentTimeMillis();

            if (messages != null)
                consumersMessages.add(messages);

            if (subscribed.get()) {
                backendConsumer.subscription().forEach(topicUri ->
                        getPscMetricRegistryManager().updateHistogramMetric(
                                topicUri, PscMetrics.PSC_CONSUMER_POLL_TIME_MS_METRIC, stopTs - startTs, pscConfigurationInternal)
                );
            } else if (assigned.get()) {
                backendConsumer.assignment().forEach(topicUriPartition ->
                        getPscMetricRegistryManager().updateHistogramMetric(
                                topicUriPartition.getTopicUri(), topicUriPartition.getPartition(),
                                PscMetrics.PSC_CONSUMER_POLL_TIME_MS_METRIC, startTs - startTs, pscConfigurationInternal
                        ));
            }
        }

        return new InterleavingPscConsumerPollMessages<>(consumersMessages);
    }

    /**
     * Commits all consumers offsets asynchronously by delegating the call to all corresponding backend consumers.
     * This API is not thread-safe.
     *
     * @throws ConsumerException if the async commit call cannot be performed on the backend consumer for some
     *                           reason.
     */
    public void commitAsync() throws ConsumerException {
        commitAsync(new OffsetCommitCallback() {
            @Override
            public void onCompletion(Map<TopicUriPartition, MessageId> offsets, Exception exception) {
                consumerInterceptors.onCommit(offsets.values());
            }
        });
    }

    /**
     * Commits all consumers offsets asynchronously. This API is not thread-safe.
     *
     * @param offsetCommitCallback the callback for completion of offset commit.
     * @throws ConsumerException if the async commit call cannot be performed on the backend consumer for some
     *                           reason.
     */
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            for (PscBackendConsumer<K, V> backendConsumer : backendConsumers) {
                backendConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onCompletion(Map<TopicUriPartition, MessageId> offsets, Exception exception) {
                        consumerInterceptors.onCommit(offsets.values());
                        offsetCommitCallback.onCompletion(offsets, exception);
                    }
                });
            }
        } finally {
            release();
        }
    }

    /**
     * Asynchronously commits the offsets for the PSC consumer's group associated with given message ids. The PSC
     * consumer does not need to have the corresponding topic URI partitions assigned to it. This API is not
     * thread-safe.
     *
     * @param messageIds collection of message ids whose corresponding offset should be asynchronously committed.
     * @throws ConsumerException      if validation of given message ids fails (e.g. if the parameter is null, or if there
     *                                are multiple message ids associated with the same topic URI partition) or if the async commit call cannot be
     *                                performed on the backend consumer for some reason.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public void commitAsync(Collection<MessageId> messageIds) throws ConsumerException, ConfigurationException {
        commitAsync(messageIds, new OffsetCommitCallback() {
            @Override
            public void onCompletion(Map<TopicUriPartition, MessageId> offsets, Exception exception) {
                consumerInterceptors.onCommit(offsets.values());
            }
        });
    }

    /**
     * Asynchronously commits the offsets for the PSC consumer's group associated with given message ids and invokes
     * the provided callback once commit is complete. The PSC consumer does not need to have the corresponding topic
     * URI partitions assigned to it. This API is not thread-safe.
     *
     * @param messageIds           collection of message ids whose corresponding offset should be asynchronously committed.
     * @param offsetCommitCallback the callback to invoke after completion of offset commit.
     * @throws ConsumerException      if validation of given message ids fails (e.g. if the parameter is null, or if there
     *                                are multiple message ids associated with the same topic URI partition) or if the async commit call cannot be
     *                                performed on the backend consumer for some reason.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public void commitAsync(Collection<MessageId> messageIds, OffsetCommitCallback offsetCommitCallback)
            throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            validateMessageIds(messageIds);

            Map<PscBackendConsumer<K, V>, Set<MessageId>> backendConsumers =
                    getCommitBackendConsumers(messageIds, false);
            for (Map.Entry<PscBackendConsumer<K, V>, Set<MessageId>> entry : backendConsumers.entrySet()) {
                entry.getKey().commitAsync(entry.getValue(), new OffsetCommitCallback() {
                    @Override
                    public void onCompletion(Map<TopicUriPartition, MessageId> offsets, Exception exception) {
                        consumerInterceptors.onCommit(offsets.values());
                        offsetCommitCallback.onCompletion(offsets, exception);
                    }
                });
            }
        } finally {
            release();
        }
    }

    /**
     * Commits all consumers offsets synchronously by delegating the call to all corresponding backend consumers.
     * This API is not thread-safe.
     *
     * @throws ConsumerException if the commit call cannot be performed on the backend consumer for some reason.
     */
    public void commitSync() throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            Set<MessageId> messageIds;
            for (PscBackendConsumer backendConsumer : backendConsumers) {
                messageIds = backendConsumer.commitSync();
                consumerInterceptors.onCommit(messageIds);
            }
        } finally {
            release();
        }
    }

    /**
     * Commits the offsets for the PSC consumer's group associated with given message ids. The PSC consumer does not
     * need to have the corresponding topic URI partitions assigned to it. This API is not thread-safe.
     *
     * @param messageIds the collection of {@link MessageId}s whose offsets should be committed.
     * @throws ConsumerException      if there are duplicate topic URI and partition pairs in the provided message ids, or
     *                                if this PscConsumer is not assigned some topic URI and partition pairs behind the given message ids.
     * @throws WakeupException        if {@link #wakeup()} is called on this PscConsumer.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public void commitSync(Collection<MessageId> messageIds) throws ConsumerException, WakeupException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            validateMessageIds(messageIds);

            Map<PscBackendConsumer<K, V>, Set<MessageId>> backendConsumers =
                    getCommitBackendConsumers(messageIds, true);
            for (Map.Entry<PscBackendConsumer<K, V>, Set<MessageId>> entry : backendConsumers.entrySet()) {
                try {
                    entry.getKey().commitSync(entry.getValue());
                } catch (WakeupException we) {
                    if (this.wakeups.incrementAndGet() == backendConsumers.size()) {
                        // when all backend consumers are interrupted, reset wakeups and throw exception
                        this.wakeups.set(-1);
                        throw new WakeupException(we);
                    } else
                        continue;
                }
                consumerInterceptors.onCommit(entry.getValue());
            }
        } finally {
            release();
        }
    }

    /**
     * Commits the offset for the PSC consumer's group associated with given message id. The PSC consumer does not
     * need to have the corresponding topic URI partition assigned to it. This API is not thread-safe.
     * {@link #assign(Collection)}. This API is not thread-safe.
     *
     * @param messageId the {@link MessageId} whose offset should be committed.
     * @throws ConsumerException      if this PscConsumer is not assigned the topic URI and partition pair behind the
     *                                given message id.
     * @throws WakeupException        if {@link #wakeup()} is called on this PscConsumer.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public void commitSync(MessageId messageId) throws ConsumerException, WakeupException, ConfigurationException {
        commitSync(Collections.singleton(messageId));
    }

    private Map<PscBackendConsumer<K, V>, Set<MessageId>> getCommitBackendConsumers(Collection<MessageId> messageIds, boolean canWakeup) throws ConfigurationException, ConsumerException {
        // dispatch messageIds to creators based on the backend
        Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();
        Map<String, Set<MessageId>> backendToMessageIds = new HashMap<>();

        for (MessageId messageId : messageIds) {
            TopicUri topicUri = messageId.getTopicUriPartition().getTopicUri();
            backendToMessageIds.computeIfAbsent(topicUri.getBackend(), b -> new HashSet<>()).add(messageId);
        }

        Map<PscBackendConsumer<K, V>, Set<MessageId>> backendConsumerToMessageIds = new HashMap<>();
        for (Map.Entry<String, Set<MessageId>> entry : backendToMessageIds.entrySet()) {
            if (creator.containsKey(entry.getKey())) {
                for (MessageId messageId : entry.getValue()) {
                    PscBackendConsumer<K, V> backendConsumer = creator.get(entry.getKey()).getCommitConsumer(
                            environment,
                            pscConfigurationInternal,
                            consumerInterceptors,
                            messageId.getTopicUriPartition(),
                            canWakeup && wakeups.get() >= 0
                    );

                    backendConsumerToMessageIds.computeIfAbsent(backendConsumer, b -> new HashSet<>())
                            .add(messageId);
                }
            } else {
                throw new ConsumerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(entry.getKey()));
            }
        }
        return backendConsumerToMessageIds;
    }

    private Map<PscBackendConsumer<K, V>, Set<MessageId>> getAssignedBackendConsumers(Collection<MessageId> messageIds) throws ConsumerException {
        Map<PscBackendConsumer<K, V>, Set<MessageId>> backendConsumers = new HashMap<>();
        Set<MessageId> invalidMessageIds = new HashSet<>();
        PscBackendConsumer<K, V> consumer;
        if (assigned.get()) {
            for (MessageId messageId : messageIds) {
                TopicUriPartition topicUriPartition = messageId.getTopicUriPartition();
                if (topicUriPartition == null) {
                    invalidMessageIds.add(messageId);
                    continue;
                }
                consumer = assignmentMap.get(topicUriPartition);
                if (consumer == null) {
                    // this shouldn't happen, unless there's a bug in the library
                    throw new ConsumerException("No consumer exists for topic URI partition of message id: " + messageId);
                }
                backendConsumers.computeIfAbsent(consumer, c -> new HashSet<>()).add(messageId);
            }
        } else { //subscribed
            for (MessageId messageId : messageIds) {
                TopicUri topicUri = messageId.getTopicUriPartition().getTopicUri();
                if (topicUri == null) {
                    invalidMessageIds.add(messageId);
                    continue;
                }
                consumer = subscriptionMap.get(topicUri);
                if (consumer == null) {
                    // this shouldn't happen, unless there's a bug in the library
                    throw new ConsumerException("No backend consumer exists for topic URI partition of message id: "
                            + messageId);
                }
                backendConsumers.computeIfAbsent(consumer, c -> new HashSet<>()).add(messageId);
            }
        }

        if (!invalidMessageIds.isEmpty()) {
            throw new ConsumerException("These message ids correspond to topic URIs or partitions not in the current " +
                    "subscription or assignment of the PSC consumer.");
        }
        return backendConsumers;
    }

    private void validateAssignment(TopicUri topicUri) throws ConsumerException {
        if (assigned.get()) {
            try {
                validateAssignment(getPartitions(topicUri.getTopicUriAsString()));
            } catch (Exception e) {
                throw new ConsumerException(e);
            }
        } else if (subscribed.get()) {
            if (!subscriptionMap.containsKey(topicUri)) {
                throw new ConsumerException(String.format(
                        "PSC consumer is not subscribed to the topic: %s", topicUri
                ));
            }
        } else
            throw new ConsumerException("PSC consumer is not subscribed to the URI or assigned the partition");
    }

    private void validateAssignment(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            if (topicUriPartition.getPartition() < 0)
                throw new ConsumerException("Invalid TopicUriPartition: " + topicUriPartition);
        }

        if (assigned.get()) {
            for (TopicUriPartition topicUriPartition : topicUriPartitions) {
                if (!assignmentMap.containsKey(topicUriPartition)) {
                    throw new ConsumerException(
                            String.format("PSC consumer is not assigned the partition: %s", topicUriPartition)
                    );
                }
            }
        } else if (subscribed.get()) {
            for (TopicUri topicUri : topicUriPartitions.stream().map(TopicUriPartition::getTopicUri).collect(Collectors.toSet()))
                validateAssignment(topicUri);
        } else
            throw new ConsumerException(ExceptionMessage.NO_SUBSCRIPTION_ASSIGNMENT);
    }

    private void validateAssignment(TopicUriPartition topicUriPartition) throws ConsumerException {
        validateAssignment(Collections.singleton(topicUriPartition));
    }

    private Set<TopicUriPartition> getDuplicateTopicUriPartitions(Collection<MessageId> messageIds) {
        Set<TopicUriPartition> visitedTopicUriPartitions = new HashSet<>();
        Set<TopicUriPartition> duplicateTopicUriPartitions = new HashSet<>();
        for (MessageId messageId : messageIds) {
            TopicUriPartition topicUriPartition = messageId.getTopicUriPartition();
            if (visitedTopicUriPartitions.contains(topicUriPartition))
                duplicateTopicUriPartitions.add(topicUriPartition);
            else
                visitedTopicUriPartitions.add(topicUriPartition);
        }
        return duplicateTopicUriPartitions;
    }

    /**
     * Moves the consumer pointer(s) (e.g. on each topic URI and partition pair) to the coordinate associated with the
     * given message ids. Note that the given message ids are assumed to be associated with the last processed messages.
     * Therefore, for each given message id, the message at the next offset will be the next one to consume. This API is
     * not thread-safe.
     *
     * @param messageIds the message ids to which the topic URI and partition pointers should be moved.
     * @throws ConsumerException if there are duplicate topic URI and partition pairs in the provided message
     *                           ids, or if this PscConsumer is not assigned some topic URI and partition pairs behind the given message ids.
     */
    public void seek(Collection<MessageId> messageIds) throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            validateMessageIds(messageIds);

            Map<PscBackendConsumer<K, V>, Set<MessageId>> backendConsumers = getAssignedBackendConsumers(messageIds);
            for (Map.Entry<PscBackendConsumer<K, V>, Set<MessageId>> entry : backendConsumers.entrySet())
                entry.getKey().seek(entry.getValue());
        } finally {
            release();
        }
    }

    /**
     * Moves the consumer pointer(s) on the given topic URI partitions to the given corresponding offset. This API is
     * not thread-safe.
     *
     * @param topicUriPartitionOffsets the map of partition to offset
     * @throws ConsumerException if this PscConsumer is not assigned any of the provided topic URI and partition pairs.
     */
    public void seekToOffset(Map<TopicUriPartition, Long> topicUriPartitionOffsets) throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            validateTopicUriPartitions(topicUriPartitionOffsets.keySet());
            Map<TopicUriPartition, Long> finalizedTopicUriPartitionOffsets = new HashMap<>();
            for (Map.Entry<TopicUriPartition, Long> entry : topicUriPartitionOffsets.entrySet())
                finalizedTopicUriPartitionOffsets.put(validateTopicUriPartition(entry.getKey()), entry.getValue());

            validateAssignment(finalizedTopicUriPartitionOffsets.keySet());
            for (Map.Entry<PscBackendConsumer<K, V>, Set<TopicUriPartition>> entry :
                    getConsumerToTopicUriPartitions(finalizedTopicUriPartitionOffsets.keySet()).entrySet()) {
                entry.getKey().seekToOffset(
                        finalizedTopicUriPartitionOffsets.entrySet()
                                .stream()
                                .filter(map -> entry.getValue().contains(map.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                );
            }
        } finally {
            release();
        }
    }

    /**
     * Moves the consumer pointer (e.g. on each topic URI and partition pair) to the coordinate associated with the
     * given message id. Note that the given message id is assumed to be associated with the last processed message.
     * Therefore, the message at the next offset will be the next one to consume. This API is not thread-safe.
     *
     * @param messageId the message id to which the topic URI and partition pointer should be moved.
     * @throws ConsumerException if this PscConsumer is not assigned the topic URI and partition of the given
     *                           message ids.
     */
    public void seek(MessageId messageId) throws ConsumerException {
        seek(Collections.singleton(messageId));
    }

    /**
     * Moves the consumer pointer (e.g. on each topic URI and partition pair) to the coordinate associated with the
     * given topic URI partition and offset. This API is not thread-safe.
     *
     * @param topicUriPartition the partition on which seek is to be performed
     * @param offset            the offset to seek to on the given partition
     * @throws ConsumerException if this PscConsumer is not assigned the given topic URI partition.
     */
    public void seekToOffset(TopicUriPartition topicUriPartition, long offset) throws ConsumerException {
        seekToOffset(new HashMap<TopicUriPartition, Long>() {
            {
                put(topicUriPartition, offset);
            }
        });
    }

    /**
     * Moves all consumer pointers associated with the given topic URI to the first coordinate with timestamp equal or
     * bigger than the given timestamp. If the consumer is using the {@link #subscribe(Collection)} API, it must already
     * be subscribed to the given URI. If it is using the {@link #assign(Collection)} API, it must be assigned all
     * partitions of the given URI. This API is not thread-safe.
     * @deprecated
     * Please use <code>seekToTimestamp(String, long)</code> instead.
     *
     * @param topicUriStr the topic URI to perform seek on
     * @param timestamp   the timestamp to which the seek is intended
     * @throws ConsumerException      if this PscConsumer is not subscribed to the given URI, or is not assigned all
     *                                partitions of the given URI, or the given URI can not be parsed, or if an exception from the backend bubbles up.
     * @throws WakeupException        if {@link #wakeup()} is called on this PscConsumer.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    @Deprecated
    public void seek(String topicUriStr, long timestamp) throws ConsumerException, WakeupException, ConfigurationException {
        seekToTimestamp(topicUriStr, timestamp);
    }

    /**
     * Moves all consumer pointers associated with the given topic URI to the first coordinate with timestamp equal or
     * bigger than the given timestamp. If the consumer is using the {@link #subscribe(Collection)} API, it must already
     * be subscribed to the given URI. If it is using the {@link #assign(Collection)} API, it must be assigned all
     * partitions of the given URI. This API is not thread-safe.
     *
     * @param topicUriStr the topic URI to perform seek on
     * @param timestamp   the timestamp to which the seek is intended
     * @throws ConsumerException      if this PscConsumer is not subscribed to the given URI, or is not assigned all
     *                                partitions of the given URI, or the given URI can not be parsed, or if an exception from the backend bubbles up.
     * @throws WakeupException        if {@link #wakeup()} is called on this PscConsumer.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public void seekToTimestamp(String topicUriStr, long timestamp) throws ConsumerException, WakeupException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            TopicUri topicUri = validateTopicUri(topicUriStr);
            validateAssignment(topicUri);
            if (assigned.get()) {
                for (TopicUriPartition topicUriPartition : getPartitions(topicUriStr))
                    seek(topicUriPartition, timestamp);
            } else { //subscribed
                PscBackendConsumer<K, V> consumer = subscriptionMap.get(topicUri);
                if (consumer != null) {
                    consumer.seekToTimestamp(topicUri, timestamp);
                    return;
                }
                // this shouldn't happen, unless there's a bug in the library
                throw new ConsumerException("Consumer does not exist for topicUri " + topicUriStr);
            }
        } finally {
            release();
        }
    }

    /**
     * Moves consumer pointers associated with the given partitions to the first coordinate with timestamp equal or
     * bigger than the corresponding timestamp. If the consumer is using the {@link #subscribe(Collection)} API, it must
     * already be subscribed to all the corresponding URIs. If it is using the {@link #assign(Collection)} API, it must
     * be assigned all the given partitions. This API is not thread-safe.
     * @deprecated
     * Please use <code>seekToTimestamp(Map&lt;TopicUriPartition, Long&gt;)</code> instead.
     *
     * @param topicUriPartitionTimestamps the map of partition to timestamp
     * @throws ConsumerException if this PscConsumer is not subscribed to any of the URIs, or is not assigned all
     *                           the given partitions, or any of the URIs can not be parsed, or if an exception from the backend bubbles up.
     */
    @Deprecated
    public void seek(Map<TopicUriPartition, Long> topicUriPartitionTimestamps) throws ConsumerException {
        seekToTimestamp(topicUriPartitionTimestamps);
    }

    /**
     * Moves consumer pointers associated with the given partitions to the first coordinate with timestamp equal or
     * bigger than the corresponding timestamp. If the consumer is using the {@link #subscribe(Collection)} API, it must
     * already be subscribed to all the corresponding URIs. If it is using the {@link #assign(Collection)} API, it must
     * be assigned all the given partitions. This API is not thread-safe.
     *
     * @param topicUriPartitionTimestamps the map of partition to timestamp
     * @throws ConsumerException if this PscConsumer is not subscribed to any of the URIs, or is not assigned all
     *                           the given partitions, or any of the URIs can not be parsed, or if an exception from the backend bubbles up.
     */
    public void seekToTimestamp(Map<TopicUriPartition, Long> topicUriPartitionTimestamps) throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            validateTopicUriPartitions(topicUriPartitionTimestamps.keySet());
            Map<TopicUriPartition, Long> finalizedTopicUriPartitionTimestamps = new HashMap<>();
            for (Map.Entry<TopicUriPartition, Long> entry : topicUriPartitionTimestamps.entrySet())
                finalizedTopicUriPartitionTimestamps.put(validateTopicUriPartition(entry.getKey()), entry.getValue());

            validateAssignment(finalizedTopicUriPartitionTimestamps.keySet());
            Map<PscBackendConsumer<K, V>, Map<TopicUriPartition, Long>> consumerToSeekPositionMap = new HashMap<>();
            PscBackendConsumer<K, V> consumer;
            TopicUriPartition topicUriPartition;
            if (assigned.get()) {
                for (Map.Entry<TopicUriPartition, Long> entry : finalizedTopicUriPartitionTimestamps.entrySet()) {
                    topicUriPartition = entry.getKey();
                    consumer = assignmentMap.get(topicUriPartition);
                    if (consumer != null) {
                        consumerToSeekPositionMap.computeIfAbsent(consumer, p -> new HashMap<>()).put(topicUriPartition, entry.getValue());
                    } else {
                        // this shouldn't happen, unless there's a bug in the library
                        throw new ConsumerException("Consumer does not exist for topicUriPartition " + entry.getKey());
                    }
                }
            } else { //subscribed
                for (Map.Entry<TopicUriPartition, Long> entry : topicUriPartitionTimestamps.entrySet()) {
                    TopicUri topicUri = topicUriStrToTopicUri.get(entry.getKey().getTopicUriAsString());
                    consumer = subscriptionMap.get(topicUri);
                    if (consumer != null) {
                        consumerToSeekPositionMap.computeIfAbsent(consumer, p -> new HashMap<>()).put(entry.getKey(), entry.getValue());
                    } else {
                        // this shouldn't happen, unless there's a bug in the library
                        throw new ConsumerException("Consumer does not exist for topicUriPartition " + entry.getKey());
                    }
                }
            }

            for (Map.Entry<PscBackendConsumer<K, V>, Map<TopicUriPartition, Long>> entry : consumerToSeekPositionMap.entrySet())
                entry.getKey().seekToTimestamp(entry.getValue());
        } finally {
            release();
        }
    }

    /**
     * Moves consumer pointer associated with the given partition to the first coordinate with timestamp equal or
     * bigger than the given timestamp. If the consumer is using the {@link #subscribe(Collection)} API, it must
     * already be subscribed to the corresponding URI. If it is using the {@link #assign(Collection)} API, it must
     * be assigned the given partition. This API is not thread-safe.
     * @deprecated
     * Please use <code>seekToTimestamp(TopicUriPartition, long)</code> instead.
     *
     * @param topicUriPartition the partition on which seek is to be performed
     * @param timestamp         the timestamp to seek to on the given partition
     * @throws ConsumerException if this PscConsumer is not subscribed to any of the URIs, or is not assigned all
     *                           the given partitions, or any of the URIs can not be parsed, or if an exception from the backend bubbles up.
     */
    @Deprecated
    public void seek(TopicUriPartition topicUriPartition, long timestamp) throws ConsumerException {
        seekToTimestamp(topicUriPartition, timestamp);
    }

    /**
     * Moves consumer pointer associated with the given partition to the first coordinate with timestamp equal or
     * bigger than the given timestamp. If the consumer is using the {@link #subscribe(Collection)} API, it must
     * already be subscribed to the corresponding URI. If it is using the {@link #assign(Collection)} API, it must
     * be assigned the given partition. This API is not thread-safe.
     *
     * @param topicUriPartition the partition on which seek is to be performed
     * @param timestamp         the timestamp to seek to on the given partition
     * @throws ConsumerException if this PscConsumer is not subscribed to any of the URIs, or is not assigned all
     *                           the given partitions, or any of the URIs can not be parsed, or if an exception from the backend bubbles up.
     */
    public void seekToTimestamp(TopicUriPartition topicUriPartition, long timestamp) throws ConsumerException {
        seek(new HashMap<TopicUriPartition, Long>() {
            {
                put(topicUriPartition, timestamp);
            }
        });
    }

    private Map<PscBackendConsumer<K, V>, Set<TopicUriPartition>> getConsumerToTopicUriPartitions(
            Collection<TopicUriPartition> topicUriPartitions
    ) throws ConsumerException {
        Map<PscBackendConsumer<K, V>, Set<TopicUriPartition>> consumerToTopicUriPartitions = new HashMap<>();
        PscBackendConsumer<K, V> consumer;
        if (assigned.get()) {
            for (TopicUriPartition topicUriPartition : topicUriPartitions) {
                consumer = assignmentMap.get(topicUriPartition);
                if (consumer != null) {
                    consumerToTopicUriPartitions.computeIfAbsent(consumer, p -> new HashSet<>()).add(topicUriPartition);
                } else {
                    // this shouldn't happen, unless there's a bug in the library
                    throw new ConsumerException("Consumer does not exist for topicUriPartition " + topicUriPartition);
                }
            }
        } else { //subscribed
            for (TopicUriPartition topicUriPartition : topicUriPartitions) {
                TopicUri topicUri = topicUriStrToTopicUri.get(topicUriPartition.getTopicUriAsString());
                consumer = subscriptionMap.get(topicUri);
                if (consumer != null) {
                    consumerToTopicUriPartitions.computeIfAbsent(consumer, p -> new HashSet<>()).add(topicUriPartition);
                } else {
                    // this shouldn't happen, unless there's a bug in the library
                    throw new ConsumerException("Consumer does not exist for topicUriPartition " + topicUriPartition);
                }
            }
        }

        return consumerToTopicUriPartitions;
    }

    /**
     * Moves consumer pointer associated with the given partitions to the beginning of the log. If the consumer is using
     * the {@link #subscribe(Collection)} API, it must already be subscribed to the corresponding URIs. If it is using
     * the {@link #assign(Collection)} API, it must be assigned all the given partitions. This API is not thread-safe.
     *
     * @param topicUriPartitions the partitions on which seek to beginning is to be performed
     * @throws ConsumerException if this PscConsumer is not subscribed to any of the URIs, or is not assigned all
     *                           the given partitions, or any of the URIs can not be parsed, or if an exception from the backend bubbles up.
     */
    public void seekToBeginning(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            topicUriPartitions = validateTopicUriPartitions(topicUriPartitions);
            validateAssignment(topicUriPartitions);

            for (Map.Entry<PscBackendConsumer<K, V>, Set<TopicUriPartition>> entry :
                    getConsumerToTopicUriPartitions(topicUriPartitions).entrySet())
                entry.getKey().seekToBeginning(entry.getValue());
        } finally {
            release();
        }
    }

    /**
     * Moves consumer pointer associated with the given partitions to the end of the log. If the consumer is using the
     * {@link #subscribe(Collection)} API, it must already be subscribed to the corresponding URIs. If it is using the
     * {@link #assign(Collection)} API, it must be assigned all the given partitions. This API is not thread-safe.
     *
     * @param topicUriPartitions the partitions on which seek to end is to be performed
     * @throws ConsumerException if this PscConsumer is not subscribed to any of the URIs, or is not assigned all
     *                           the given partitions, or any of the URIs can not be parsed, or if an exception from the backend bubbles up.
     */
    public void seekToEnd(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        acquireAndEnsureOpen();
        try {
            topicUriPartitions = validateTopicUriPartitions(topicUriPartitions);
            validateAssignment(topicUriPartitions);

            for (Map.Entry<PscBackendConsumer<K, V>, Set<TopicUriPartition>> entry :
                    getConsumerToTopicUriPartitions(topicUriPartitions).entrySet())
                entry.getKey().seekToEnd(entry.getValue());
        } finally {
            release();
        }
    }

    /**
     * Retrieves all partitions associated with the given URI. The PscConsumer does not need to be subscribed to the
     * URI to call this API. This API is not thread-safe.
     *
     * @param topicUriStr the URI for which the associated partitions are to be returned.
     * @return a set of topic URI partitions corresponding to the given topic URI; null if the topic URI does not exist.
     * @throws ConsumerException      if the given URI can not be parsed, or if an error from the backend bubbles up.
     * @throws WakeupException        if {@link #wakeup()} is called on this PscConsumer.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    @SuppressWarnings("unchecked")
    public Set<TopicUriPartition> getPartitions(String topicUriStr) throws ConsumerException, WakeupException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            TopicUri topicUri = validateTopicUri(topicUriStr);

            // dispatch topicUris to creators based on the backend
            @SuppressWarnings("rawtypes")
            Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();

            if (creator.containsKey(topicUri.getBackend())) {
                PscBackendConsumer<K, V> backendConsumer = creator.get(topicUri.getBackend()).getConsumer(
                        environment,
                        pscConfigurationInternal,
                        consumerInterceptors,
                        topicUri,
                        wakeups.get() >= 0,
                        false
                );

                try {
                    return backendConsumer.getPartitions(topicUri);
                } catch (WakeupException we) {
                    this.wakeups.set(-1);
                    throw new WakeupException(we);
                }
            } else {
                throw new ConsumerException("[PSC] Cannot handle topicUri with backend " + topicUriStr);
            }
        } finally {
            release();
        }
    }

    /**
     * Returns the topic RNs available in the cluster that match the given regex pattern. Instead of returning
     * a list of TopicURIs as usual, we return RNs. The main difference between URIs and RNs in PSC is that we
     * prefix the transport protocol to RNs {@code <protocol>:/<topicRn>}. Since we don't subscribe to any topics
     * using this API and we're merely fetchig metadata, we can't discover the protocol to properly create the TopicURI
     *
     * @param baseTopicRn the base RN for service discovery
     * @param pattern the pattern to filter out the topic names to return
     * @return
     * @throws ConsumerException
     * @throws ConfigurationException
     */
    public Set<String> listTopicRNs(String baseTopicRn, Pattern pattern)
        throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            // transport and topic name don't matter since we're only trying to initialize the
            // consumer
            String topicUriStr = "plaintext:/" + baseTopicRn + ":topic";
            TopicUri topicUri = validateTopicUri(topicUriStr);
            Set<String> matchedTopicRns = new HashSet<>();

            // dispatch topicUris to creators based on the backend
            @SuppressWarnings("rawtypes")
            Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();

            if (creator.containsKey(topicUri.getBackend())) {
                PscBackendConsumer<K, V>
                    backendConsumer =
                    creator.get(topicUri.getBackend()).getConsumer(
                        environment,
                        pscConfigurationInternal,
                        consumerInterceptors,
                        topicUri,
                        wakeups.get() >= 0,
                        false
                    );
                backendConsumer.getTopicNames().forEach(topic -> {
                    if (pattern.matcher(topic).matches()) {
                        matchedTopicRns.add(baseTopicRn + ":" + topic);
                    }
                });
                return matchedTopicRns;
            } else {
                throw new ConsumerException(
                    "[PSC] Cannot handle topicUri with backend " + topicUriStr);
            }
        } finally {
            release();
        }
    }

    private void internalClose(Optional<Duration> optionalTimeout) throws ConsumerException {
        if (this.subscribed.get()) {
            if (optionalTimeout.isPresent())
                unsubscribe(optionalTimeout.get());
            else
                unsubscribe();
        }

        if (this.assigned.get()) {
            if (optionalTimeout.isPresent())
                unassign(optionalTimeout.get());
            else
                unassign();
        }

        if (closed.getAndSet(true)) {
            // avoid multiple closes
            return;
        }

        if (listenerExecutor != null)
            listenerExecutor.shutdown();
        creatorManager.reset();

        // Testing metrics
        //PscMetricRegistryManager.getInstance().reportToConsole();
        PscMetricRegistryManager.getInstance().shutdown(pscConfigurationInternal);
        logger.info("PSC consumer was closed.");
        logger.clearContext();
    }

    /**
     * Closes this PscConsumer instance. It also unsubscribes/unassigns the consumer from any URI/partition associated
     * with the consumer.
     *
     * @param timeout the timeout for closing each of the registered backend consumers.
     * @throws ConsumerException if the potential call to {@link #unsubscribe()} or {@link #unassign()} throws an
     *                           error.
     */
    public void close(Duration timeout) throws ConsumerException {
        if (timeout.toMillis() < 0)
            throw new ConsumerException("Timeout cannot be negative.");
        acquire();
        try {
            internalClose(Optional.of(timeout));
        } finally {
            release();
        }
    }

    /**
     * Closes this PscConsumer instance. It also unsubscribes/unassigns the consumer from any URI/partition associated
     * with the consumer.
     *
     * @throws ConsumerException if the potential call to {@link #unsubscribe()} or {@link #unassign()} throws an
     *                           error.
     */
    public void close() throws ConsumerException {
        acquire();
        try {
            internalClose(Optional.empty());
        } finally {
            release();
        }
    }

    @VisibleForTesting
    protected void setCreatorManager(PscConsumerCreatorManager creatorManager) {
        this.creatorManager = creatorManager;
    }

    @VisibleForTesting
    protected void setPscMetricRegistryManager(PscMetricRegistryManager pscMetricRegistryManager) {
        this.pscMetricRegistryManager = pscMetricRegistryManager;
    }

    @VisibleForTesting
    protected PscMetricRegistryManager getPscMetricRegistryManager() {
        if (pscMetricRegistryManager == null)
            pscMetricRegistryManager = PscMetricRegistryManager.getInstance();

        return pscMetricRegistryManager;
    }

    public ConsumerRebalanceListener getRebalanceListener() {
        return this.rebalanceListener;
    }

    /**
     * Interrupts the PscConsumer in case it is stuck in a long running {@link #poll()} call. It delegates the call to
     * all registered backend consumers. This API is thread-safe.
     */
    public void wakeup() {
        // if the previous wakeup() call is not completed yet (not interrupted the consumer poll yet) return
        if (this.wakeups.getAndUpdate(value -> Math.max(value, 0)) >= 0)
            return;

        for (PscBackendConsumer<K, V> backendConsumer : backendConsumers)
            backendConsumer.wakeup();
    }

    /**
     * Returns a message id associated with the committed position of the given topic URI partition.
     *
     * @param topicUriPartition the topic URI partition for which the committed position is to be returned.
     * @return a message id from the backend consumer that encompasses the requested offset, or null if there is no
     *         prior commit.
     * @throws ConsumerException      if the given URI can not be parsed, or if an error from the backend bubbles up.
     * @throws WakeupException        if {@link #wakeup()} is called on this PscConsumer.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public MessageId committed(TopicUriPartition topicUriPartition) throws ConsumerException, WakeupException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            topicUriPartition = validateTopicUriPartition(topicUriPartition);

            // dispatch topicUris to creators based on the backend
            @SuppressWarnings("rawtypes")
            Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();
            TopicUri topicUri = topicUriPartition.getTopicUri();
            if (creator.containsKey(topicUri.getBackend())) {
                PscBackendConsumer<K, V> backendConsumer = creator.get(topicUri.getBackend()).getConsumer(
                        environment,
                        pscConfigurationInternal,
                        consumerInterceptors,
                        topicUri,
                        wakeups.get() >= 0,
                        false
                );

                try {
                    return backendConsumer.committed(topicUriPartition);
                } catch (WakeupException we) {
                    this.wakeups.set(-1);
                    throw new WakeupException(we);
                }
            } else {
                throw new ConsumerException("[PSC] Cannot process topicUri: " + topicUriPartition.getTopicUriAsString());
            }
        } finally {
            release();
        }
    }

    // TODO: unit test this
    public Collection<MessageId> committed(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            // maintain a map of backend consumer to topicUriPartitions
            Map<PscBackendConsumer<K, V>, Collection<TopicUriPartition>> tupToBackendConsumerMap = new HashMap<>();
            for (TopicUriPartition topicUriPartition: topicUriPartitions) {
                topicUriPartition = validateTopicUriPartition(topicUriPartition);
                Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();
                TopicUri topicUri = topicUriPartition.getTopicUri();
                if (creator.containsKey(topicUri.getBackend())) {
                    PscBackendConsumer<K, V> backendConsumer = creator.get(topicUri.getBackend()).getConsumer(
                            environment,
                            pscConfigurationInternal,
                            consumerInterceptors,
                            topicUri,
                            wakeups.get() >= 0,
                            false
                    );
                    tupToBackendConsumerMap.computeIfAbsent(backendConsumer, k -> new HashSet<>());
                    tupToBackendConsumerMap.get(backendConsumer).add(topicUriPartition);
                } else {
                    throw new ConsumerException("[PSC] Cannot process topicUri: " + topicUriPartition.getTopicUriAsString());
                }
            }

            Collection<MessageId> messageIds = new ArrayList<>();
            for (Map.Entry<PscBackendConsumer<K, V>, Collection<TopicUriPartition>> entry : tupToBackendConsumerMap.entrySet()) {
                Collection<MessageId> subset = entry.getKey().committed(entry.getValue());
                messageIds.addAll(subset);
            }

            return messageIds;
        } finally {
            release();
        }
    }

    /**
     * Returns the log start offset of the request topic URI partitions. The contract for the returned offsets depends on
     * the backend-specific implementation. For example, for a Kafka backend, the start offset is the offset of the
     * first message in the partition.
     *
     * @param topicUriPartitions list of topic URI partitions for which the start offset is requested
     * @return a mapping of topic URI partition to start offset corresponding to the input collection
     * @throws ConsumerException      if any of the given topic URIs is invalid, or if an error from backend consumer
     *                                bubbles up.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public Map<TopicUriPartition, Long> startOffsets(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            topicUriPartitions = validateTopicUriPartitions(topicUriPartitions);

            Map<PscBackendConsumer<K, V>, Set<TopicUriPartition>> backendConsumerToTopicUriPartitions =
                    maybeCreateBackendConsumerAndReturnBackendConsumerToTopicUriPartitions(topicUriPartitions);

            Map<TopicUriPartition, Long> startOffsets = new HashMap<>();
            for (Map.Entry<PscBackendConsumer<K, V>, Set<TopicUriPartition>> entry : backendConsumerToTopicUriPartitions.entrySet())
                startOffsets.putAll(entry.getKey().startOffsets(entry.getValue()));

            return startOffsets;
        } finally {
            release();
        }
    }

    /**
     * Returns the log end offset of the request topic URI partitions. The contract for the returned offsets depends on
     * the backend-specific implementation. For example, for a Kafka backend, the end offset, for the case of
     * uncommitted reads, is the offset of the last message in the partition plus one.
     *
     * @param topicUriPartitions list of topic URI partitions for which the end offset is requested
     * @return a mapping of topic URI partition to end offset corresponding to the input collection
     * @throws ConsumerException      if any of the given topic URIs is invalid, or if an error from backend consumer
     *                                bubbles up.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public Map<TopicUriPartition, Long> endOffsets(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            topicUriPartitions = validateTopicUriPartitions(topicUriPartitions);

            Map<PscBackendConsumer<K, V>, Set<TopicUriPartition>> backendConsumerToTopicUriPartitions =
                    maybeCreateBackendConsumerAndReturnBackendConsumerToTopicUriPartitions(topicUriPartitions);

            Map<TopicUriPartition, Long> endOffsets = new HashMap<>();
            for (Map.Entry<PscBackendConsumer<K, V>, Set<TopicUriPartition>> entry : backendConsumerToTopicUriPartitions.entrySet())
                endOffsets.putAll(entry.getKey().endOffsets(entry.getValue()));

            return endOffsets;
        } finally {
            release();
        }
    }

    private Map<PscBackendConsumer<K, V>, Set<TopicUriPartition>> maybeCreateBackendConsumerAndReturnBackendConsumerToTopicUriPartitions(
            Collection<TopicUriPartition> topicUriPartitions
    ) throws ConfigurationException, ConsumerException {
        if (topicUriPartitions.isEmpty())
            return new HashMap<>();

        // dispatch topicUriPartitions to creators based on the backend
        @SuppressWarnings("rawtypes")
        Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();
        Map<String, Set<TopicUriPartition>> backendToTopicUriPartitions = new HashMap<>();

        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            TopicUri topicUri = topicUriPartition.getTopicUri();
            backendToTopicUriPartitions.computeIfAbsent(topicUri.getBackend(), b -> new HashSet<>()).add(topicUriPartition);
        }

        Map<PscBackendConsumer<K, V>, Set<TopicUriPartition>> backendConsumerToTopicUriPartitions = new HashMap<>();
        for (Map.Entry<String, Set<TopicUriPartition>> entry : backendToTopicUriPartitions.entrySet()) {
            if (creator.containsKey(entry.getKey())) {
                for (TopicUriPartition topicUriPartition : entry.getValue()) {
                    PscBackendConsumer<K, V> backendConsumer = creator.get(entry.getKey()).getAssignmentConsumer(
                            environment,
                            pscConfigurationInternal,
                            consumerInterceptors,
                            topicUriPartition,
                            wakeups.get() >= 0,
                            false
                    );

                    backendConsumerToTopicUriPartitions.computeIfAbsent(backendConsumer, b -> new HashSet<>())
                            .add(topicUriPartition);
                }
            } else {
                throw new ConsumerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(entry.getKey()));
            }
        }

        return backendConsumerToTopicUriPartitions;
    }

    /**
     * Returns the next offset of the given topic URI partition to be consumed by this consumer. The topic URI partition
     * must be assigned to the consumer. This API is not thread-safe.
     *
     * @param topicUriPartition the partition for which the position will be returned.
     * @return the next consumption offset of the given partition.
     * @throws ConsumerException if the input is not valid, or if there is an exception returned by the backend
     *                           consumer.
     * @throws WakeupException   if {@link #wakeup()} is called on this PscConsumer.
     */
    public long position(TopicUriPartition topicUriPartition) throws ConsumerException, WakeupException {
        acquireAndEnsureOpen();
        try {
            topicUriPartition = validateTopicUriPartition(topicUriPartition);
            validateAssignment(topicUriPartition);

            PscBackendConsumer<K, V> backendConsumer;
            if (assigned.get()) {
                backendConsumer = assignmentMap.get(topicUriPartition);
            } else { //subscribed
                backendConsumer = subscriptionMap.get(topicUriPartition.getTopicUri());
            }

            try {
                return backendConsumer.position(topicUriPartition);
            } catch (WakeupException we) {
                this.wakeups.set(-1);
                throw new WakeupException(we);
            }
        } finally {
            release();
        }
    }

    /**
     * Returns the message id associated with each given pair of topic URI partition and timestamp. The
     * message id will be the message id associated with the first message at or after the given timestamp.
     *
     * @param topicUriPartitionTimestamps a map of topic URI partition and timestamp pairs for which message
     *                                    ids are being queried.
     * @return a map of topic URI partition to message id.
     * @throws ConsumerException      if the input is not valid, or if there is an exception returned by the backend
     *                                consumer.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    public Map<TopicUriPartition, MessageId> getMessageIdByTimestamp(Map<TopicUriPartition, Long> topicUriPartitionTimestamps) throws ConsumerException, ConfigurationException {
        acquireAndEnsureOpen();
        try {
            validateTopicUriPartitions(topicUriPartitionTimestamps.keySet());
            Map<TopicUriPartition, Long> finalizedTopicUriPartitionTimestamps = new HashMap<>();
            for (Map.Entry<TopicUriPartition, Long> entry : topicUriPartitionTimestamps.entrySet())
                finalizedTopicUriPartitionTimestamps.put(validateTopicUriPartition(entry.getKey()), entry.getValue());
            if (finalizedTopicUriPartitionTimestamps.isEmpty())
                return new HashMap<>();

            // dispatch topicUriPartitions to creators based on the backend
            @SuppressWarnings("rawtypes")
            Map<String, PscBackendConsumerCreator> creator = creatorManager.getBackendCreators();
            Map<String, Map<TopicUriPartition, Long>> backendToTopicUriPartitionTimestampMap = new HashMap<>();

            for (Map.Entry<TopicUriPartition, Long> entry : finalizedTopicUriPartitionTimestamps.entrySet()) {
                backendToTopicUriPartitionTimestampMap.computeIfAbsent(
                        entry.getKey().getTopicUri().getBackend(), b -> new HashMap<>()
                ).put(entry.getKey(), entry.getValue());
            }

            Map<PscBackendConsumer<K, V>, Map<TopicUriPartition, Long>> backendConsumerToTopicUriPartitions = new HashMap<>();
            for (Map.Entry<String, Map<TopicUriPartition, Long>> entry : backendToTopicUriPartitionTimestampMap.entrySet()) {
                if (creator.containsKey(entry.getKey())) {
                    for (Map.Entry<TopicUriPartition, Long> entry2 : entry.getValue().entrySet()) {
                        PscBackendConsumer<K, V> backendConsumer = creator.get(entry.getKey()).getCommitConsumer(
                                environment,
                                pscConfigurationInternal,
                                consumerInterceptors,
                                entry2.getKey(),
                                false
                        );

                        backendConsumerToTopicUriPartitions.computeIfAbsent(backendConsumer, b -> new HashMap<>())
                                .put(entry2.getKey(), entry2.getValue());
                    }
                } else {
                    throw new ConsumerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(entry.getKey()));
                }
            }

            Map<TopicUriPartition, MessageId> messageIdByTopicUriPartition = new HashMap<>();
            for (Map.Entry<PscBackendConsumer<K, V>, Map<TopicUriPartition, Long>> entry : backendConsumerToTopicUriPartitions.entrySet())
                messageIdByTopicUriPartition.putAll(entry.getKey().getMessageIdByTimestamp(entry.getValue()));

            return messageIdByTopicUriPartition;
        } finally {
            release();
        }
    }

    /**
     * Returns metrics associated with this consumer by collecting metrics from all backend consumers.
     *
     * @return consumer metrics
     * @throws ConsumerException if there is a validation failure or collecting metric from backend consumers fails.
     */
    public Map<MetricName, Metric> metrics() throws ClientException {
        ensureOpen();
        Map<MetricName, Metric> metrics = new ConcurrentHashMap<>();
        for (PscBackendConsumer<K, V> backendConsumer : backendConsumers) {
            metrics.putAll(backendConsumer.metrics());
        }
        return metrics;
    }

    private TopicUri validateTopicUri(String topicUriAsString) throws ConsumerException {
        if (topicUriAsString == null)
            throw new ConsumerException("Null topic URI was passed to the consumer API.");

        if (topicUriStrToTopicUri.containsKey(topicUriAsString))
            return topicUriStrToTopicUri.get(topicUriAsString);

        try {
            TopicUri convertedTopicUri = TopicUri.validate(topicUriAsString);
            Map<String, PscBackendConsumerCreator> backendCreators = creatorManager.getBackendCreators();
            String backend = convertedTopicUri.getBackend();
            if (!backendCreators.containsKey(backend))
                throw new ConsumerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(backend));
            convertedTopicUri = backendCreators.get(backend).validateBackendTopicUri(convertedTopicUri);
            topicUriStrToTopicUri.put(topicUriAsString, convertedTopicUri);
            return convertedTopicUri;
        } catch (PscStartupException e) {
            throw new ConsumerException(e.getMessage(), e);
        }
    }

    private Set<TopicUri> validateTopicUris(Collection<String> topicUrisAsString) throws ConsumerException {
        if (topicUrisAsString == null)
            throw new ConsumerException("Null topic URI was passed to the consumer API");

        Set<TopicUri> backendTopicUris = new HashSet<>();
        StringBuilder topicUriErrors = new StringBuilder();
        topicUrisAsString.forEach(
                topicUriAsString -> {
                    try {
                        backendTopicUris.add(validateTopicUri(topicUriAsString));
                    } catch (ConsumerException e) {
                        topicUriErrors.append(e.getMessage()).append("\n");
                    }
                }
        );

        if (topicUriErrors.length() > 0)
            throw new ConsumerException(topicUriErrors.toString().trim());

        return backendTopicUris;
    }

    private TopicUriPartition validateTopicUriPartition(TopicUriPartition topicUriPartition) throws ConsumerException {
        if (topicUriPartition == null)
            throw new ConsumerException("Null topic URI partition was passed to the consumer API.");

        TopicUri convertedTopicUri = validateTopicUri(topicUriPartition.getTopicUriAsString());
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, convertedTopicUri);
        return topicUriPartition;
    }

    private Set<TopicUriPartition> validateTopicUriPartitions(Set<TopicUriPartition> topicUriPartitions)
            throws ConsumerException {
        for (TopicUriPartition tup: topicUriPartitions) {
            tup = validateTopicUriPartition(tup);
        }
        return topicUriPartitions;
    }

    private Set<TopicUriPartition> validateTopicUriPartitions(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (topicUriPartitions == null)
            throw new ConsumerException("Null topic URI partitions was passed to the consumer API");

        Set<TopicUriPartition> backendTopicUriPartitions = new HashSet<>();
        StringBuilder topicUriPartitionErrors = new StringBuilder();
        topicUriPartitions.forEach(
                topicUriPartition -> {
                    try {
                        backendTopicUriPartitions.add(validateTopicUriPartition(topicUriPartition));
                    } catch (ConsumerException e) {
                        topicUriPartitionErrors.append(e.getMessage()).append("\n");
                    }
                }
        );

        if (topicUriPartitionErrors.length() > 0)
            throw new ConsumerException(topicUriPartitionErrors.toString().trim());

        return backendTopicUriPartitions;
    }

    private void validateMessageIds(Collection<MessageId> messageIds) throws ConsumerException {
        if (messageIds == null)
            throw new ConsumerException("Null message ids was passed to consumer API.");

        StringBuilder messageIdErrors = new StringBuilder();
        for (MessageId messageId : messageIds) {
            if (messageId == null)
                messageIdErrors.append("Null message id found in collection passed to consumer API.");
            else if (messageId.getTopicUriPartition() == null)
                messageIdErrors.append("Message id contains null topic URI partition.");
            else
                validateTopicUriPartition(messageId.getTopicUriPartition());
        }
        if (messageIdErrors.length() > 0)
            throw new ConsumerException(messageIdErrors.toString());

        Set<TopicUriPartition> duplicateTopicUriPartitions = getDuplicateTopicUriPartitions(messageIds);
        if (!duplicateTopicUriPartitions.isEmpty()) {
            throw new ConsumerException(
                    ExceptionMessage.DUPLICATE_PARTITIONS_IN_MESSAGE_IDS(duplicateTopicUriPartitions)
            );
        }
    }

    public void onEvent(PscEvent event) {
        if (subscribed.get() && event.getTopicUri() != null) {
            subscriptionMap.get(event.getTopicUri()).onEvent(event);
        } else if (assigned.get() && event.getTopicUriPartition() != null) {
            assignmentMap.get(event.getTopicUriPartition()).onEvent(event);
        }
    }

    @VisibleForTesting
    protected Map<String, PscConfiguration> getBackendConsumerConfigurationPerTopicUri() {
        if (assigned.get())
            return assignmentMap.entrySet().stream().collect(Collectors.toMap(
                    entry -> entry.getKey().getTopicUriAsString(),
                    entry -> entry.getValue().getConfiguration()
            ));
        else
            return subscriptionMap.entrySet().stream().collect(Collectors.toMap(
                    entry -> entry.getKey().getTopicUriAsString(),
                    entry -> entry.getValue().getConfiguration()
            ));
    }

    @VisibleForTesting
    protected PscConfigurationInternal getPscConfiguration() {
        return pscConfigurationInternal;
    }
}