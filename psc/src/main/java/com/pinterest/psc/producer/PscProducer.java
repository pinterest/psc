package com.pinterest.psc.producer;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.MetricsReporterConfiguration;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationReporter;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.producer.TransactionalProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.PscStartupException;
import com.pinterest.psc.interceptor.Interceptors;
import com.pinterest.psc.interceptor.TypePreservingInterceptor;
import com.pinterest.psc.interceptor.ProducerInterceptors;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.producer.creation.PscBackendProducerCreator;
import com.pinterest.psc.producer.creation.PscProducerCreatorManager;
import com.pinterest.psc.serde.Serializer;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PscProducer<K, V> implements AutoCloseable {
    private static final PscLogger logger = PscLogger.getLogger(PscProducer.class);

    static {
        logger.addContext("pscPid", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        logger.addContext("pscTid", String.valueOf(Thread.currentThread().getId()));
        logger.addContext("pscHost", PscCommon.getHostname());
    }

    private Environment environment;
    private PscProducerCreatorManager creatorManager;
    private Interceptors<K, V> interceptors;
    private ProducerInterceptors<K, V> producerInterceptors;

    // flags
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // This should keep the up-to-date URI prefix to producer mapping across API calls
    private final Map<String, PscBackendProducer<K, V>> pscBackendProducerByTopicUriPrefix = new ConcurrentHashMap<>();
    // This should keep the up-to-date transactional state of each backend producer
    private final Map<PscBackendProducer<K, V>, TransactionalState> transactionalStateByBackendProducer = new ConcurrentHashMap<>();
    // This keeps the transactional state of the PSC producer itself, valid states: NON_TRANSACTIONAL, INIT_AND_BEGUN
    private final AtomicReference<TransactionalState> transactionalState = new AtomicReference<>(TransactionalState.NON_TRANSACTIONAL);
    // This is a translation map from a topic URI (string format) to parsed uris
    private final Map<String, TopicUri> topicUriStrToTopicUri = new HashMap<>();
    // This is the source of truth of the currently registered consumers
    private final Set<PscBackendProducer<K, V>> backendProducers = new HashSet<>();

    private PscMetricRegistryManager pscMetricRegistryManager;
    private final PscConfigurationInternal pscConfigurationInternal;

    /**
     * Creates a PscProducer object using the configuration in the provided config file to override the default
     * PscProducer configurations.
     *
     * @param customPscConfigurationFilePath the file containing overridden configuration in key=value lines.
     * @throws ConfigurationException in case there are validation errors with the configuration.
     * @throws ProducerException      in case there are errors when initializing the PscProducer instance.
     */
    public PscProducer(String customPscConfigurationFilePath) throws ConfigurationException, ProducerException {
        if (customPscConfigurationFilePath == null)
            throw new ProducerException("Null parameter was passed to API PscProducer(String)");
        pscConfigurationInternal = new PscConfigurationInternal(customPscConfigurationFilePath, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);
        initialize();
    }

    /**
     * Creates a PscProducer object using the configuration in the provided configuration object to override the default
     * PscProducer configurations.
     * @deprecated
     * Please use <code>PscProducer(PscConfiguration)</code> instead.
     *
     * @param configuration the config object that contains all overridden configurations.
     * @throws ConfigurationException in case there are validation errors with the configuration.
     * @throws ProducerException      in case there are errors when initializing the PscProducer instance.
     */
    @Deprecated
    public PscProducer(Configuration configuration) throws ConfigurationException, ProducerException {
        if (configuration == null)
            throw new ProducerException("Null parameter was passed to API PscProducer(Configuration)");
        pscConfigurationInternal = new PscConfigurationInternal(configuration, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);
        initialize();
    }

    /**
     * Creates a PscProducer object using the configuration in the provided configuration object to override the default
     * PscProducer configurations.
     *
     * @param pscConfiguration the config object that contains all overridden configurations.
     * @throws ConfigurationException in case there are validation errors with the configuration.
     * @throws ProducerException      in case there are errors when initializing the PscProducer instance.
     */
    public PscProducer(PscConfiguration pscConfiguration) throws ConfigurationException, ProducerException {
        if (pscConfiguration == null)
            throw new ProducerException("Null parameter was passed to API PscProducer(PscConfiguration)");
        pscConfigurationInternal = new PscConfigurationInternal(pscConfiguration, PscConfiguration.PSC_CLIENT_TYPE_PRODUCER);
        initialize();
    }

    /**
     * @return the configured client id of this {@link PscProducer}
     */
    public String getClientId() {
        return pscConfigurationInternal.getPscProducerClientId();
    }

    @VisibleForTesting
    protected Interceptors<K, V> getInterceptors() {
        return interceptors;
    }

    @SuppressWarnings("unchecked")
    private void initialize() throws ConfigurationException {
        Serializer<K> keySerializer = pscConfigurationInternal.getPscProducerKeySerializer();
        Serializer<V> valueSerializer = pscConfigurationInternal.getPscProducerValueSerializer();

        List<TypePreservingInterceptor<byte[], byte[]>> rawInterceptors = pscConfigurationInternal.getRawPscProducerInterceptors();
        List<TypePreservingInterceptor<K, V>> typedInterceptors = pscConfigurationInternal.getTypedPscProducerInterceptors()
                .stream().map(interceptor -> (TypePreservingInterceptor<K, V>) interceptor).collect(Collectors.toList());

        interceptors = new Interceptors<>(rawInterceptors, typedInterceptors, pscConfigurationInternal);
        producerInterceptors = new ProducerInterceptors<>(interceptors, keySerializer, valueSerializer);

        creatorManager = new PscProducerCreatorManager();
        environment = pscConfigurationInternal.getEnvironment();

        initializeMetricsReporting();
        PscMetricRegistryManager.getInstance().incrementCounterMetric(null, PscMetrics.PSC_PRODUCER_COUNT, pscConfigurationInternal);
    }

    /**
     * This method can be internally used by <code>PscProducer</code> in situations where the API call on the backend
     * producer throws exceptions that are not usually temporary and can only be fixed by closing the backend producer
     * and starting another one with similar properties.
     *
     * @param pscBackendProducer
     * @throws ProducerException
     * @throws ConfigurationException
     */
    @VisibleForTesting
    protected void reset(PscBackendProducer<K, V> pscBackendProducer) throws ProducerException, ConfigurationException {
        ensureOpen();

        if (pscBackendProducer == null)
            return;

        // first properly terminate and close the backend producer, and remove it from internal state of PSC producer
        TransactionalState backendProducerTransactionalState = transactionalStateByBackendProducer.get(pscBackendProducer);
        pscBackendProducer.wakeup();
        try {
            pscBackendProducer.close();
        } catch (ProducerException producerException) {
            logger.error("Backend producer failed to close.");
            throw producerException;
        } catch (Exception exception) {
            throw new ProducerException("Backend producer failed to close.", exception);
        }

        backendProducers.remove(pscBackendProducer);
        transactionalStateByBackendProducer.remove(pscBackendProducer);
        Iterator<Map.Entry<String, PscBackendProducer<K, V>>> iterator = pscBackendProducerByTopicUriPrefix.entrySet().iterator();
        Set<String> topicUriStrs = new HashSet<>();
        while (iterator.hasNext()) {
            Map.Entry<String, PscBackendProducer<K, V>> next = iterator.next();
            if (next.getValue() == pscBackendProducer) {
                topicUriStrs.add(next.getKey());
                iterator.remove();
            }
        }

        // extract the topic URI object
        String backend = null;
        if (topicUriStrs.isEmpty())
            throw new ProducerException("Invalid state: Could not find the to-be-reset backend producer in the internal state.");
        Set<TopicUri> topicUris = new HashSet<>();
        for (Map.Entry<String, TopicUri> entry : topicUriStrToTopicUri.entrySet()) {
            for (String topicUriStr : topicUriStrs) {
                if (entry.getKey().startsWith(topicUriStr)) {
                    topicUris.add(entry.getValue());
                    backend = entry.getValue().getBackend();
                    break;
                }
            }
        }
        if (topicUris.isEmpty())
            throw new ProducerException("Invalid state: Expected topic URIs could not be found in the internal state.");

        // then remove it from the backend consumer creator map
        Map<String, PscBackendProducerCreator> creator = creatorManager.getBackendCreators();
        if (creator.containsKey(backend)) {
            creator.get(backend).dismissBackendProducer(topicUris, pscBackendProducer);
        } else {
            throw new ProducerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(
                    String.join(", ", topicUriStrs)
            ));
        }

        // verify the transactional state and throw if the state is incompatible
        switch (backendProducerTransactionalState) {
            case INIT_AND_BEGUN:
            case IN_TRANSACTION:
            case BEGUN:
                throw new TransactionalProducerException("Transaction states of PSC producer and its backend producers " +
                        "do not allow for a clean backend producer reset. This means the transaction needs to be " +
                        "aborted and retried.");
            case NON_TRANSACTIONAL:
            case READY:
                // ok. resume execution of reset below
        }

        // bring the new backend producer to the same transaction state of the one that was dismissed
        PscBackendProducer<K, V> backendProducer = getBackendProducerForTopicUri(topicUris.iterator().next());
        if (backendProducerTransactionalState.equals(TransactionalState.READY))
            backendProducer.initTransaction();
        transactionalStateByBackendProducer.put(backendProducer, backendProducerTransactionalState);
    }

    /**
     * Emits a single PSC producer message to the proper backend based on the topic URI information in the message.
     *
     * @param pscProducerMessage the message that includes all information necessary to publish the message to the proper
     *                           backend.
     * @return a future that contains the message id associated with the produced message.
     * @throws ProducerException      if the given topic URI fails validation, or if issues from backend producer bubble up.
     * @throws ConfigurationException if discovery of proper backend for the given topic URI fails.
     */
    public Future<MessageId> send(PscProducerMessage<K, V> pscProducerMessage) throws ProducerException, ConfigurationException {
        return send(pscProducerMessage, null);
    }

    /**
     * Emits a single PSC producer message to the proper backend based on the topic URI information in the message.
     * Upon completion of the send call it triggers the callback, if provided.
     *
     * @param pscProducerMessage the message that includes all information necessary to publish the message to the proper
     *                           backend.
     * @param callback           the callback that should be triggered after send is complete
     * @return a future that contains the message id associated with the produced message.
     * @throws ProducerException      if the given topic URI fails validation, or if issues from backend producer bubble up.
     * @throws ConfigurationException if discovery of proper backend for the given topic URI fails.
     */
    public Future<MessageId> send(PscProducerMessage<K, V> pscProducerMessage, Callback callback) throws ProducerException, ConfigurationException {
        ensureOpen();
        validateProducerMessage(pscProducerMessage);
        if (transactionalState.get() != TransactionalState.NON_TRANSACTIONAL &&
                !backendProducers.isEmpty() &&
                !pscBackendProducerByTopicUriPrefix.containsKey(pscProducerMessage.getTopicUriPartition().getTopicUri().getTopicUriPrefix())) {
            throw new ProducerException("Invalid call to send() which would have created a new backend producer. This is not allowed when the PscProducer" +
                    " is already transactional.");
        }
        PscBackendProducer<K, V> backendProducer =
                getBackendProducerForTopicUri(pscProducerMessage.getTopicUriPartition().getTopicUri());

        TransactionalState state = transactionalStateByBackendProducer.get(backendProducer);
        switch (state) {
            case NON_TRANSACTIONAL:
            case IN_TRANSACTION:
                break;
            case READY:
                throw new ProducerException("Invalid transaction state: call to send() when producer is in ready state.");
            case INIT_AND_BEGUN:
                try {
                    backendProducer.initTransaction();
                    transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.INIT_AND_BEGUN, TransactionalState.READY);
                } catch (Exception exception) {
                    transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.NON_TRANSACTIONAL);
                    logger.error("Initializing transactions on backend producer failed.");
                    throw exception;
                }
                // state == READY
                try {
                    backendProducer.beginTransaction();
                    // beginning and sending: READY -> BEGUN -> IN_TRANSACTION
                    transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY, TransactionalState.IN_TRANSACTION);
                } catch (Exception exception) {
                    transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY);
                    logger.error("beginTransaction() on backend producer failed.");
                    throw exception;
                }
                break;
            case BEGUN:
                transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.BEGUN, TransactionalState.IN_TRANSACTION);
                break;
        }

        PscMetricRegistryManager.getInstance().incrementCounterMetric(
                pscProducerMessage.getTopicUriPartition().getTopicUri(),
                pscProducerMessage.getPartition(),
                PscMetrics.PSC_PRODUCER_BACKEND_SEND_ATTEMPT_COUNT,
                pscConfigurationInternal
        );

        Future<MessageId> future = backendProducer.send(pscProducerMessage, callback);

        return future;
    }

    /**
     * Retrieves all partitions associated with the given URI.
     *
     * @param topicUriStr the URI for which the associated partitions are to be returned.
     * @return a set of topic URI partitions corresponding to the given topic URI; null if the topic URI does not exist.
     * @throws ProducerException      if the given URI can not be parsed, or if an error from the backend bubbles up.
     * @throws ConfigurationException if the discovery of backend cluster fails.
     */
    @SuppressWarnings("unchecked")
    public Set<TopicUriPartition> getPartitions(String topicUriStr) throws ProducerException, ConfigurationException {
        ensureOpen();
        TopicUri topicUri = validateTopicUri(topicUriStr);
        PscBackendProducer<K, V> backendProducer = getBackendProducerForTopicUri(topicUri);
        return backendProducer.getPartitions(topicUri);
    }

    /**
     * Aborts the active transaction of this producer.
     *
     * @throws ProducerException if the producer is already closed, or is not in the proper state to perform an abort,
     *                           or there is an error bubbling up from the backend.
     */
    @InterfaceStability.Evolving
    public void abortTransaction() throws ProducerException {
        ensureOpen();

        // the case where no backend producer is created yet
        if (backendProducers.isEmpty()) {
            if (transactionalState.get().equals(TransactionalState.NON_TRANSACTIONAL))
                throw new ProducerException("Invalid transaction state: call to abortTransaction() before initializing transactions.");
            if (transactionalState.compareAndSet(TransactionalState.INIT_AND_BEGUN, TransactionalState.NON_TRANSACTIONAL))
                return;
            logger.error("Unexpected transactional state of PscProducer: {}", transactionalState.get().toString());
        }

        // the case with backend producers present
        for (PscBackendProducer<K, V> backendProducer : backendProducers) {
            if (transactionalStateByBackendProducer.get(backendProducer).equals(TransactionalState.NON_TRANSACTIONAL))
                throw new ProducerException("Invalid transaction state: call to abortTransaction() before initializing transactions.");
            if (transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.INIT_AND_BEGUN, TransactionalState.NON_TRANSACTIONAL)) {
                transactionalState.set(TransactionalState.NON_TRANSACTIONAL);
                return;
            }
            if (transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY, TransactionalState.BEGUN))
                throw new ProducerException("Invalid transaction state: call to abortTransaction() before transaction begun.");

            // state == IN_TRANSACTION | BEGUN
            transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY);
            backendProducer.abortTransaction();
        }
    }

    /**
     * Prepares the state of this producer to start a transaction.
     *
     * @throws ProducerException if the producer is already closed, or is not in the proper state to begin a
     *                           transaction.
     */
    @InterfaceStability.Evolving
    public void beginTransaction() throws ProducerException {
        ensureOpen();

        // the case where no backend producer is created yet
        if (backendProducers.isEmpty()) {
            if (transactionalState.compareAndSet(TransactionalState.NON_TRANSACTIONAL, TransactionalState.INIT_AND_BEGUN))
                return;
            if (transactionalState.compareAndSet(TransactionalState.INIT_AND_BEGUN, TransactionalState.NON_TRANSACTIONAL))
                throw new ProducerException("Invalid transaction state: consecutive calls to beginTransaction().");
            logger.error("Unexpected transactional state of PscProducer: {}", transactionalState.get().toString());
        }

        // the case with backend producers present
        for (PscBackendProducer<K, V> backendProducer : backendProducers) {
            if (transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.NON_TRANSACTIONAL, TransactionalState.INIT_AND_BEGUN)) {
                transactionalState.set(TransactionalState.INIT_AND_BEGUN);
                return;
            }
            if (transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.INIT_AND_BEGUN, TransactionalState.NON_TRANSACTIONAL)) {
                transactionalState.set(TransactionalState.NON_TRANSACTIONAL);
                throw new ProducerException("Invalid transaction state: consecutive calls to beginTransaction().");
            }
            if (transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.IN_TRANSACTION, TransactionalState.READY))
                throw new ProducerException("Invalid transaction state: call to beginTransaction() while transaction is in progress.");
            if (transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.BEGUN, TransactionalState.READY))
                throw new ProducerException("Invalid transaction state: consecutive calls to beginTransaction().");

            // state == READY
            try {
                backendProducer.beginTransaction();
                if (!transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY, TransactionalState.BEGUN))
                    logger.error("Expected transaction state: READY. Actual state: {}",
                            transactionalStateByBackendProducer.get(backendProducer));
            } catch (Exception exception) {
                transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.NON_TRANSACTIONAL);
                logger.error("beginTransaction() on backend producer failed.");
                throw exception;
            }
        }
    }

    /**
     * Initializes the transactional producer in the backend. This moves the transactional state one step further from what
     * <code>beginTransaction()</code> does as it creates a backend producer and initializes its transactional state. This
     * is used in cases where the PSC producer needs to be immediately transactional ready upon creation. Note that this
     * call makes the PSC producer transactional, which means follow-up calls to <code>beingTransaction()</code> will fail.
     *
     * @param topicUriString the topic URI the producer will be sending messages to. It is used to determine against which
     *                       backend the backend producer needs to be created.
     * @return transactional properties of the backend producer after initialization.
     * @throws ProducerException if the producer is already closed, or is not in the proper state to perform a commit, or
     *                           there is an error bubbling up from the backend.
     * @throws ConfigurationException if discovery of proper backend for the given topic URI fails.
     */
    @InterfaceStability.Evolving
    protected PscProducerTransactionalProperties initTransactions(String topicUriString) throws ProducerException, ConfigurationException {
        ensureOpen();

        TopicUri topicUri = validateTopicUri(topicUriString);
        PscBackendProducer<K, V> backendProducer = getBackendProducerForTopicUri(topicUri);
        initTransactions(backendProducer);
        return backendProducer.getTransactionalProperties();
    }

    /**
     * Centralized logic for initializing transactions for a given backend producer.
     *
     * @param backendProducer the backendProducer to initialize transactions for
     * @throws ProducerException if the producer is already closed, or is not in the proper state to initialize transactions
     */
    protected void initTransactions(PscBackendProducer<K, V> backendProducer) throws ProducerException {
        if (!transactionalStateByBackendProducer.get(backendProducer).equals(TransactionalState.NON_TRANSACTIONAL) &&
                !transactionalStateByBackendProducer.get(backendProducer).equals(TransactionalState.INIT_AND_BEGUN))
            throw new ProducerException("Invalid transaction state: initializing transactions works only once for a PSC producer.");

        try {
            backendProducer.initTransaction();
            if (transactionalStateByBackendProducer.get(backendProducer).equals(TransactionalState.NON_TRANSACTIONAL))
                transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.NON_TRANSACTIONAL, TransactionalState.READY);
            if (transactionalStateByBackendProducer.get(backendProducer).equals(TransactionalState.INIT_AND_BEGUN))
                transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.INIT_AND_BEGUN, TransactionalState.READY);
            transactionalState.set(TransactionalState.INIT_AND_BEGUN);
        } catch (Exception exception) {
            transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.NON_TRANSACTIONAL);
            logger.error("initTransactions() on backend producer failed.");
            throw exception;
        }

        this.beginTransaction();
    }

    /**
     * Commits the active transaction of this producer.
     *
     * @throws ProducerException if the producer is already closed, or is not in the proper state to perform a commit,
     *                           or there is an error bubbling up from the backend.
     */
    @InterfaceStability.Evolving
    public void commitTransaction() throws ProducerException {
        ensureOpen();

        // the case where no backend producer is created yet
        if (backendProducers.isEmpty()) {
            if (transactionalState.get().equals(TransactionalState.NON_TRANSACTIONAL))
                throw new ProducerException("Invalid transaction state: call to commitTransaction() before initializing transactions.");
            if (transactionalState.compareAndSet(TransactionalState.INIT_AND_BEGUN, TransactionalState.NON_TRANSACTIONAL))
                return;
            logger.error("Unexpected transactional state of PscProducer: {}", transactionalState.get().toString());
        }

        // the case with backend producers present
        for (PscBackendProducer<K, V> backendProducer : backendProducers) {
            if (transactionalStateByBackendProducer.get(backendProducer).equals(TransactionalState.NON_TRANSACTIONAL))
                throw new ProducerException("Invalid transaction state: call to commitTransaction() before initializing transactions.");
            if (transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.INIT_AND_BEGUN, TransactionalState.NON_TRANSACTIONAL)) {
                transactionalState.set(TransactionalState.NON_TRANSACTIONAL);
                return;
            }
            if (transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY, TransactionalState.BEGUN))
                throw new ProducerException("Invalid transaction state: call to commitTransaction() before transaction begun.");

            // state == IN_TRANSACTION | BEGUN
            transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY);
            try {
                backendProducer.commitTransaction();
            } catch (Exception exception) {
                logger.error("commitTransaction() on backend producer failed.");
                throw exception;
            }
        }
    }

    /**
     * Keeps the given topic URI partition offsets (from the message id) to commit against the given consumer group
     * when the transactional producer commits.
     *
     * @param offsets                 pairs of topic URI paritions and message id (from the consumed messages).
     * @param consumerGroupId         the consumer group against which offsets to be committed.
     * @throws ProducerException      if the given topic URI partitions fail validation, or if issues from backend producer bubble up.
     * @throws ConfigurationException if discovery of proper backend for the given topic URI partitions fails.
     */
    @InterfaceStability.Evolving
    public void sendOffsetsToTransaction(Map<TopicUriPartition, MessageId> offsets, String consumerGroupId)
            throws ProducerException, ConfigurationException {
        ensureOpen();

        Map<PscBackendProducer<K, V>, Map<TopicUriPartition, Long>> offsetsByBackendProducer = new HashMap<>();
        for (Map.Entry<TopicUriPartition, MessageId> entry : offsets.entrySet()) {
            TopicUriPartition topicUriPartition = entry.getKey();
            topicUriPartition = validateTopicUriPartition(topicUriPartition);
            MessageId messageId = entry.getValue();
            PscBackendProducer<K, V> backendProducer = getBackendProducerForTopicUri(topicUriPartition.getTopicUri());
            offsetsByBackendProducer.computeIfAbsent(backendProducer, b -> new HashMap<>())
                    .put(topicUriPartition, messageId.getOffset());
        }

        for (Map.Entry<PscBackendProducer<K, V>, Map<TopicUriPartition, Long>> entry : offsetsByBackendProducer.entrySet()) {
            PscBackendProducer<K, V> backendProducer = entry.getKey();

            TransactionalState state = transactionalStateByBackendProducer.get(backendProducer);
            switch (state) {
                case NON_TRANSACTIONAL:
                    throw new ProducerException("Invalid transaction state: call to sendOffsetsToTransaction() on a non-transactional producer.");
                case IN_TRANSACTION:
                    break;
                case READY:
                    throw new ProducerException("Invalid transaction state: call to sendOffsetsToTransaction() when producer is in ready state.");
                case INIT_AND_BEGUN:
                    try {
                        backendProducer.initTransaction();
                        transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.INIT_AND_BEGUN, TransactionalState.READY);
                    } catch (Exception exception) {
                        transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.NON_TRANSACTIONAL);
                        logger.error("Initializing transactions on backend producer failed.");
                        throw exception;
                    }
                    // state == READY
                    try {
                        backendProducer.beginTransaction();
                        // beginning and sending: READY -> BEGUN -> IN_TRANSACTION
                        transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY, TransactionalState.IN_TRANSACTION);
                    } catch (Exception exception) {
                        transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.READY);
                        logger.error("beginTransaction() on backend producer failed.");
                        throw exception;
                    }
                    break;
                case BEGUN:
                    transactionalStateByBackendProducer.replace(backendProducer, TransactionalState.INIT_AND_BEGUN, TransactionalState.IN_TRANSACTION);
                    break;
            }

            try {
                backendProducer.sendOffsetsToTransaction(entry.getValue(), consumerGroupId);
            } catch (Exception exception) {
                logger.error("Backend producer failed to send offsets to transaction:" +
                        " [offsetByTopicUriPartition: {}, GroupId:{}].", PscUtils.toString(entry.getValue()), consumerGroupId);
                throw exception;
            }
        }
    }

    /**
     * Resumes an active transaction that is in progress by another {@link PscProducer} instance.
     *
     * @param otherPscProducer the PSC producer that this PSC producer should take over its active transaction
     * @throws ProducerException      if the producer is already closed, or there is an error bubbling up from the backend
     *                                while adjusting the state.
     * @throws ConfigurationException if retrieving backend producer fails
     */
    @InterfaceStability.Evolving
    public void resumeTransaction(PscProducer otherPscProducer) throws ProducerException, ConfigurationException {
        ensureOpen();

        if (otherPscProducer == null || otherPscProducer.closed.get() || !otherPscProducer.isInTransaction())
            return;

        for (TopicUri topicUri : (Collection<TopicUri>) otherPscProducer.topicUriStrToTopicUri.values()) {
            // for each backend producer of other psc producer
            PscBackendProducer otherBackendProducer = (PscBackendProducer)
                    otherPscProducer.pscBackendProducerByTopicUriPrefix.get(topicUri.getTopicUriPrefix());
            // create a similar backend producer for this psc producer
            PscBackendProducer<K, V> backendProducer = getBackendProducerForTopicUri(topicUri);
            // set the transactional state accordingly
            TransactionalState otherTransactionalState = (TransactionalState)
                    otherPscProducer.transactionalStateByBackendProducer.get(otherBackendProducer);
            transactionalStateByBackendProducer.put(backendProducer, otherTransactionalState);
            // have the created backend producer resume transaction of other backend producer if necessary
            switch (otherTransactionalState) {
                case NON_TRANSACTIONAL:
                case INIT_AND_BEGUN:
                    continue;
                case IN_TRANSACTION:
                case READY:
                case BEGUN:
                    backendProducer.resumeTransaction(otherBackendProducer);
                    transactionalStateByBackendProducer.put(backendProducer, TransactionalState.IN_TRANSACTION);
            }
        }

        transactionalState.set(TransactionalState.INIT_AND_BEGUN);
    }

    /**
     * Resumes a transaction from its checkpointed state.
     *
     * @param pscProducerTransactionalProperties the backend-compatible properties necessary to recover a transaction
     * @param topicUris the set of topic URIs associated with the producer
     * @throws ProducerException      if the producer is already closed, or there is an error bubbling up from the backend
     *                                while recovering the state.
     */
    @InterfaceStability.Evolving
    public void resumeTransaction(PscProducerTransactionalProperties pscProducerTransactionalProperties, Set<String> topicUris) throws ProducerException {
        if (topicUris == null || topicUris.isEmpty())
            throw new ProducerException("Resuming transaction cannot proceed due to missing topic URIs");

        ensureOpen();

        boolean resumed = false;
        PscBackendProducer<K, V> pscBackendProducer;
        for (String topicUriString : topicUris) {
            TopicUri topicUri = validateTopicUri(topicUriString);
            pscBackendProducer = null;
            try {
                pscBackendProducer = getBackendProducerForTopicUri(topicUri);
            } catch (ConfigurationException | ProducerException exception) {
                logger.warn("Could not extract a backend producer for topic uri '{}'", topicUriString, exception);
                // TODO: verify that not throwing is okay - assuming not all passed topic uris may be associated to the stored transaction state.
            }

            if (pscBackendProducer != null) {
                try {
                    pscBackendProducer.resumeTransaction(pscProducerTransactionalProperties);
                    transactionalStateByBackendProducer.put(pscBackendProducer, TransactionalState.IN_TRANSACTION);
                    resumed = true;
                } catch (ProducerException exception) {
                    logger.warn("Resuming transaction failed for backend producer '{}'", pscBackendProducer, exception);
                    // TODO: verify that not throwing is okay - assuming not all passed topic uris may be associated to the stored transaction state.
                }
            }
        }

        if (!resumed)
            throw new ProducerException("Resuming transaction failed. Check earlier warnings for potential root cause.");

        transactionalState.set(TransactionalState.INIT_AND_BEGUN);
    }

    /**
     * This API is added due to a dependency by Flink connector, and should not be normally used by a typical producer.
     *
     * @return all objects representing the backend transaction managers for the active backend producers.
     * @throws ProducerException if the backend producer throws an exception handling the call.
     */
    @InterfaceStability.Evolving
    public Set<Object> getTransactionManagers() throws ProducerException {
        Set<Object> transactionManagers = new HashSet<>(backendProducers.size());
        for (PscBackendProducer backendProducer : backendProducers) {
            transactionManagers.add(backendProducer.getTransactionManager());
        }
        return transactionManagers;
    }

    /**
     * Get exactly one transaction manager. If there is more than one transaction managers / backend producers,
     * this method will throw an exception. Note that this is added due to a dependency by Flink connector,
     * and should not need to be used otherwise.
     *
     * @return the transaction manager object
     * @throws ProducerException if there is an error in the backend producer or if there is more than one transaction managers
     */
    @InterfaceStability.Evolving
    protected Object getExactlyOneTransactionManager() throws ProducerException {
        Set<Object> transactionManagers = getTransactionManagers();
        if (transactionManagers.size() != 1)
            throw new ProducerException("Expected exactly one transaction manager, but found " + transactionManagers.size());
        return transactionManagers.iterator().next();
    }

    /**
     * This API is added due to a dependency by Flink connector, and should not be normally used by a typical producer.
     *
     * @return an object representing the backend transaction manager for the given producer message.
     * @throws ProducerException if the backend producer throws an exception handling the call.
     */
    @InterfaceStability.Evolving
    public Object getTransactionManager(PscProducerMessage pscProducerMessage) throws ProducerException {
        String topicUriPrefix = topicUriStrToTopicUri.get(pscProducerMessage.getTopicUriAsString()).getTopicUriPrefix();
        return pscBackendProducerByTopicUriPrefix.get(topicUriPrefix).getTransactionManager();
    }

    /**
     * Wakes up the thread that is responsible for sending requests to the backend pubsub. This is added due to a
     * dependency by Flink connector, and should not be normally used by a typical producer.
     *
     * @throws ProducerException if there is a validation failure or the backend producer throws an exception
     * handling the wakeup call.
     */
    public void wakeup() throws ProducerException {
        ensureOpen();
        for (PscBackendProducer backendProducer : backendProducers) {
            backendProducer.wakeup();
        }
    }

    /**
     * Returns metrics associated with this producer by collecting metrics from all backend producers.
     *
     * @return producer metrics
     * @throws ClientException if there is a validation failure or collecting metric from backend
     * producers fails.
     */
    public Map<MetricName, Metric> metrics() throws ClientException {
        ensureOpen();
        Map<MetricName, Metric> metrics = new HashMap<>();
        for (PscBackendProducer<K, V> backendProducer : backendProducers) {
            metrics.putAll(backendProducer.metrics());
        }
        return metrics;
    }

    /**
     * This causes all buffered messages to be sent immediately and blocks until their send completion. It can be used
     * to skip the batching period imposed by {@value PscConfiguration#PSC_PRODUCER_BATCH_DURATION_MAX_MS}.
     *
     * @throws ProducerException if the producer is closed or there is an error from the backend producer.
     */
    public void flush() throws ProducerException {
        ensureOpen();
        for (PscBackendProducer<K, V> backendProducer : backendProducers)
            backendProducer.flush();
    }

    /**
     * Closes this PscProducer instance.
     *
     * @throws ProducerException if closing some backend producer fails
     */
    @Override
    public void close() throws ProducerException {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * Closes this PscProducer instance after the given timeout (maximum) for incomplete requests to complete.
     * Any request not completed by the given timeout will fail.
     *
     * @param duration maximum time that each backend producer should wait for incomplete requests.
     * @throws ProducerException if closing some backend producer fails.
     */
    public void close(Duration duration) throws ProducerException {
        if (closed.getAndSet(true)) {
            // avoid multiple closes
            return;
        }

        creatorManager.reset();

        List<Exception> exceptions = new ArrayList<>();
        for (Map.Entry<String, PscBackendProducer<K, V>> entry : pscBackendProducerByTopicUriPrefix.entrySet()) {
            try {
                backendProducers.remove(entry.getValue());
                transactionalStateByBackendProducer.remove(entry.getValue());
                pscBackendProducerByTopicUriPrefix.remove(entry.getKey()).close(duration);
            } catch (Exception exception) {
                exceptions.add(exception);
            }
        }

        transactionalState.set(TransactionalState.NON_TRANSACTIONAL);

        if (!exceptions.isEmpty()) {
            throw new ProducerException(
                    String.format(
                            "Some backend producers failed to close.\n%s",
                            exceptions.stream().map(Throwable::getMessage).collect(Collectors.joining("\n"))
                    )
            );
        }

        if (PscConfigurationReporter.isThisYou(pscConfigurationInternal.getConfiguration()))
            logger.info("PSC configuration reporter was closed.");
        else {
            logger.info("PSC producer was closed.");
            PscMetricRegistryManager.getInstance().shutdown(pscConfigurationInternal);
        }
        logger.clearContext();
    }

    /**
     * Checks whether the producer is in the middle of active transaction.
     *
     * @return true if the producer is in the middle of a transaction; and false otherwise.
     */
    @InterfaceStability.Evolving
    public boolean isInTransaction() {
        if (backendProducers.isEmpty())
            return false;

        for (PscBackendProducer<K, V> backendProducer : backendProducers) {
            switch (transactionalStateByBackendProducer.get(backendProducer)) {
                case IN_TRANSACTION:
                case READY:
                case BEGUN:
                    return true;
                default:
                    // no-op
            }
        }

        return false;
    }

    private List<TypePreservingInterceptor<K, V>> initializeTypedInterceptors(
            String[] pscProducerInterceptorTypedClasses)
            throws ConfigurationException {
        List<TypePreservingInterceptor<K, V>> typedInterceptors = new ArrayList<>();
        for (String typedInterceptorClass : pscProducerInterceptorTypedClasses) {
            TypePreservingInterceptor<K, V> typedInterceptor = PscUtils.instantiateFromClass(
                    typedInterceptorClass, TypePreservingInterceptor.class
            );
            typedInterceptor.configure(pscConfigurationInternal.getConfiguration());
            typedInterceptors.add(typedInterceptor);
        }
        return typedInterceptors;
    }

    private List<TypePreservingInterceptor<byte[], byte[]>> initializeRawInterceptors(
            String[] pscProducerInterceptorsRawClasses)
            throws ConfigurationException {
        List<TypePreservingInterceptor<byte[], byte[]>> rawInterceptors = new ArrayList<>();
        for (String rawInterceptorClass : pscProducerInterceptorsRawClasses) {
            TypePreservingInterceptor<byte[], byte[]> rawInterceptor = PscUtils.instantiateFromClass(
                    rawInterceptorClass, TypePreservingInterceptor.class
            );
            rawInterceptor.configure(pscConfigurationInternal.getConfiguration());
            rawInterceptors.add(rawInterceptor);
        }
        return rawInterceptors;
    }

    private void initializeMetricsReporting() {
        if (PscConfigurationReporter.isThisYou(pscConfigurationInternal.getConfiguration()))
            return;
        MetricsReporterConfiguration metricsReporterConfiguration = new MetricsReporterConfiguration(
                pscConfigurationInternal.isPscMetricsReportingEnabled(),
                pscConfigurationInternal.getPscMetricsReporterClass(),
                pscConfigurationInternal.getConfiguration().getInt(PscConfiguration.PSC_METRICS_REPORTER_PARALLELISM),
                pscConfigurationInternal.getConfiguration().getString(PscConfiguration.PSC_METRICS_HOST),
                pscConfigurationInternal.getConfiguration().getInt(PscConfiguration.PSC_METRICS_PORT),
                pscConfigurationInternal.getConfiguration().getInt(PscConfiguration.PSC_METRICS_FREQUENCY_MS)
        );

        PscMetricRegistryManager.getInstance().initialize(pscConfigurationInternal);
        PscMetricRegistryManager.getInstance().enableJvmMetrics("_jvm", pscConfigurationInternal);

    }

    protected void ensureOpen() throws ProducerException {
        if (closed.get())
            throw new ProducerException(ExceptionMessage.ALREADY_CLOSED_EXCEPTION);
    }

    private TopicUri validateTopicUri(String topicUriAsString) throws ProducerException {
        if (topicUriAsString == null)
            throw new ProducerException("Null topic URI was passed to the producer API.");

        if (topicUriStrToTopicUri.containsKey(topicUriAsString))
            return topicUriStrToTopicUri.get(topicUriAsString);

        try {
            TopicUri convertedTopicUri = TopicUri.validate(topicUriAsString);
            Map<String, PscBackendProducerCreator> backendCreators = creatorManager.getBackendCreators();
            String backend = convertedTopicUri.getBackend();
            if (!backendCreators.containsKey(backend))
                throw new ProducerException(ExceptionMessage.TOPIC_URI_UNSUPPORTED_BACKEND(backend));
            convertedTopicUri = backendCreators.get(backend).validateBackendTopicUri(convertedTopicUri);
            topicUriStrToTopicUri.put(topicUriAsString, convertedTopicUri);
            return convertedTopicUri;
        } catch (PscStartupException e) {
            throw new ProducerException(e.getMessage(), e);
        }
    }

    private TopicUriPartition validateTopicUriPartition(TopicUriPartition topicUriPartition) throws ProducerException {
        if (topicUriPartition == null)
            throw new ProducerException("Null topic URI partition was passed to the consumer API.");

        TopicUri convertedTopicUri = validateTopicUri(topicUriPartition.getTopicUriAsString());
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, convertedTopicUri);
        return topicUriPartition;
    }

    private void validateProducerMessage(PscProducerMessage<K, V> pscProducerMessage) throws ProducerException {
        TopicUri topicUri = validateTopicUri(pscProducerMessage.getTopicUriAsString());
        TopicUriPartition topicUriPartition = new TopicUriPartition(
                pscProducerMessage.getTopicUriAsString(), pscProducerMessage.getPartition()
        );
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
        pscProducerMessage.setTopicUriPartition(topicUriPartition);
    }

    protected PscBackendProducer<K, V> getBackendProducerForTopicUri(TopicUri topicUri) throws ProducerException, ConfigurationException {
        topicUriStrToTopicUri.put(topicUri.getTopicUriAsString(), topicUri);

        if (pscBackendProducerByTopicUriPrefix.containsKey(topicUri.getTopicUriPrefix()))
            return pscBackendProducerByTopicUriPrefix.get(topicUri.getTopicUriPrefix());

        // dispatch topicUri to creator based on the backend
        Map<String, PscBackendProducerCreator> creator = creatorManager.getBackendCreators();
        if (creator.containsKey(topicUri.getBackend())) {
            PscBackendProducer<K, V> backendProducer = creator.get(topicUri.getBackend()).getProducer(
                    environment,
                    pscConfigurationInternal,
                    producerInterceptors,
                    topicUri
            );

            pscBackendProducerByTopicUriPrefix.put(topicUri.getTopicUriPrefix(), backendProducer);
            transactionalStateByBackendProducer.put(backendProducer, transactionalState.get());
            backendProducers.add(backendProducer);
            return backendProducer;
        } else {
            throw new ProducerException("[PSC] Cannot process topicUri: " + topicUri);
        }
    }

    @VisibleForTesting
    protected void setCreatorManager(PscProducerCreatorManager creatorManager) {
        this.creatorManager = creatorManager;
    }

    @VisibleForTesting
    protected void setPscMetricRegistryManager(PscMetricRegistryManager metricRegistryManager) {
        this.pscMetricRegistryManager = metricRegistryManager;
    }

    @VisibleForTesting
    protected Collection<PscBackendProducer<K, V>> getBackendProducers() {
        return backendProducers;
    }

    @VisibleForTesting
    protected PscBackendProducer<K, V> getBackendProducer(String topicUriString) throws ProducerException {
        TopicUri topicUri = validateTopicUri(topicUriString);
        return pscBackendProducerByTopicUriPrefix.get(topicUri.getTopicUriPrefix());
    }

    @VisibleForTesting
    @InterfaceStability.Evolving
    protected TransactionalState getTransactionalState() {
        return transactionalState.get();
    }

    @VisibleForTesting
    @InterfaceStability.Evolving
    protected TransactionalState getBackendProducerState(PscBackendProducer<K, V> backendProducer) {
        return transactionalStateByBackendProducer.get(backendProducer);
    }

    @VisibleForTesting
    protected PscMetricRegistryManager getPscMetricRegistryManager() {
        if (pscMetricRegistryManager == null)
            pscMetricRegistryManager = PscMetricRegistryManager.getInstance();

        return pscMetricRegistryManager;
    }

    @VisibleForTesting
    protected PscConfigurationInternal getPscConfiguration() {
        return pscConfigurationInternal;
    }

    @VisibleForTesting
    protected enum TransactionalState {
        NON_TRANSACTIONAL,
        INIT_AND_BEGUN,
        READY,
        IN_TRANSACTION,
        BEGUN
    }
}
