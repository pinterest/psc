package com.pinterest.psc.producer.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscMessage;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaErrors;
import com.pinterest.psc.common.kafka.KafkaMessageId;
import com.pinterest.psc.common.kafka.KafkaSslUtils;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscProducerToKafkaProducerConfigConverter;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.exception.producer.BackendProducerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.metrics.kafka.KafkaMetricsHandler;
import com.pinterest.psc.metrics.kafka.KafkaUtils;
import com.pinterest.psc.producer.Callback;
import com.pinterest.psc.producer.PscBackendProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.producer.PscProducerTransactionalProperties;
import com.pinterest.psc.producer.transaction.TransactionManagerUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PscKafkaProducer<K, V> extends PscBackendProducer<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(PscKafkaProducer.class);
    //private static final PscLogger chargebackLogger = PscLogger.getLogger("chargeback.logger");
    final Map<KafkaProducer<byte[], byte[]>, Boolean> allProducers = new ConcurrentHashMap<>();
    final Map<KafkaProducer<byte[], byte[]>, Integer> references = new ConcurrentHashMap<>();
    final AtomicBoolean potentialLeak = new AtomicBoolean(false);
    final Semaphore mutex = new Semaphore(1);
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private String configuredPscProducerId;
    private Properties properties;
    private long sslCertificateExpiryTimeInMillis;
    //private String project;

    @Override
    public void initialize(PscConfigurationInternal pscConfigurationInternal, ServiceDiscoveryConfig discoveryConfig, Environment environment, TopicUri topicUri) {
        //project = pscConfigurationInternal.getProject();
        properties = new PscProducerToKafkaProducerConfigConverter().convert(pscConfigurationInternal, topicUri);
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                discoveryConfig.getConnect()
        );
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName()
        );
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName()
        );

        maybeAdjustConfiguration();
        configuredPscProducerId = pscConfigurationInternal.getPscProducerClientId();
        kafkaProducer = getNewKafkaProducer();

        // if using secure protocol (SSL), calculate cert expiry time
        if (pscConfigurationInternal.isProactiveSslResetEnabled() &&
                topicUri.getProtocol().equals(KafkaTopicUri.SECURE_PROTOCOL)) {
            sslCertificateExpiryTimeInMillis = KafkaSslUtils.calculateSslCertExpiryTime(
                    properties, pscConfigurationInternal, Collections.singleton(topicUri));
            logger.info("Initialized PscKafkaProducer with proactive SSL certificate reset. " +
                    "Expiry time at " + sslCertificateExpiryTimeInMillis);
        } else {
            logger.info("Initialized PscKafkaProducer without proactive SSL certificate reset.");
        }
        super.initialize(pscConfigurationInternal, discoveryConfig, environment, topicUri);
    }

    private KafkaProducer<byte[], byte[]> getNewKafkaProducer() {
        return getNewKafkaProducer(false);
    }

    private KafkaProducer<byte[], byte[]> getNewKafkaProducer(boolean bumpRef) {
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, configuredPscProducerId + "-" + UUID.randomUUID());
        kafkaProducer = new KafkaProducer<>(properties);
        // we set PID and epoch explicitly here to avoid ProducerFencedException when a new producer is created
        // they must be -1 in order for initTransaction to succeed - we don't yet know why
        // in some cases they are not -1 even though they should be initialized to -1 on calling the KafkaProducer
        // constructor
        try {
            // the transactionManager will be non-null iff the producer enables idempotence and transactions
            if (getTransactionManager() != null) {
                setProducerId(-1);
                setEpoch((short) -1);
            }
        } catch (ProducerException e) {
            logger.warn("Error in setting producer ID and epoch." +
                    " This might be ok if the producer won't be transactional.", e);
        }
        updateStatus(kafkaProducer, true);
        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                null,
                PscMetrics.PSC_PRODUCER_BACKEND_COUNT, pscConfigurationInternal
        );
        if (bumpRef)
            references.merge(kafkaProducer, 1, Integer::sum);
        return kafkaProducer;
    }

    private KafkaProducer<byte[], byte[]> getProducer() throws InterruptedException {
        mutex.acquire();
        try {
            if (kafkaProducer == null) {
                return getNewKafkaProducer(true);
            } else {
                if (!getStatus(kafkaProducer)) {
                    return getNewKafkaProducer(true);
                } else {
                    references.merge(kafkaProducer, 1, Integer::sum);
                    return kafkaProducer;
                }
            }
        } finally {
            mutex.release();
        }
    }

    private void updateStatus(KafkaProducer<byte[], byte[]> kafkaProducer, boolean isActive) {
        updateOrGetStatus(kafkaProducer, isActive);
    }

    private boolean getStatus(KafkaProducer<byte[], byte[]> kafkaProducer) {
        return updateOrGetStatus(kafkaProducer, null);
    }

    private boolean updateOrGetStatus(KafkaProducer<byte[], byte[]> kafkaProducer, Boolean isActive) {
        synchronized (kafkaProducer) {
            if (isActive == null) {
                return allProducers.containsKey(kafkaProducer) && allProducers.get(kafkaProducer);
            } else {
                allProducers.put(kafkaProducer, isActive.booleanValue());
                return false; // no-op
            }
        }
    }

    @Override
    protected synchronized void reportProducerMetrics() {
        if (kafkaProducer == null)
            return;

        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> kafkaMetrics = kafkaProducer.metrics();
        KafkaMetricsHandler.handleKafkaClientMetrics(kafkaMetrics, backendTopicToTopicUri, true, pscConfigurationInternal);
        KafkaUtils.convertKafkaMetricsToPscMetrics(kafkaMetrics, metricValueProvider);
    }

    private void maybeAdjustConfiguration() {
        // transactional id and acks
        if (properties.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
            String acksValue = properties.getProperty(ProducerConfig.ACKS_CONFIG);
            if (!acksValue.equals("all") && !acksValue.equals("-1")) {
                logger.warn("Overwriting the producer acks config value ({}) as it must be set to all/-1 for " +
                        "transactional producer.", acksValue);
                PscMetricRegistryManager.getInstance().incrementCounterMetric(
                        null,
                        PscMetrics.PSC_BACKEND_PRODUCER_CONFIG_OVERWRITE, pscConfigurationInternal
                );
                properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
                logger.warn("Updated {}: {} -> {}.", ProducerConfig.ACKS_CONFIG, acksValue, -1);
            }
        }

        // auto resolution and retries
        if (autoResolutionEnabled) {
            int retries = properties.containsKey(ProducerConfig.RETRIES_CONFIG) ?
                    Integer.parseInt(properties.getProperty(ProducerConfig.RETRIES_CONFIG)) :
                    Integer.parseInt(ProducerConfig.configDef().defaultValues().get(ProducerConfig.RETRIES_CONFIG).toString());
            if (retries > 1000) {
                logger.warn("Overwriting the producer retries config value ({}) as it must be set to a low number " +
                        "since auto resolution is enabled ", retries);
                PscMetricRegistryManager.getInstance().incrementCounterMetric(
                        null,
                        PscMetrics.PSC_BACKEND_PRODUCER_CONFIG_OVERWRITE, pscConfigurationInternal
                );
                properties.setProperty(ProducerConfig.RETRIES_CONFIG, "100");
                logger.warn("Updated {}: {} -> {}.", ProducerConfig.RETRIES_CONFIG, retries, 100);
            }
        }
    }

    public PscKafkaProducer() {
    }

    @Override
    public Set<TopicUriPartition> getPartitions(TopicUri topicUri) throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("getPartitions()");

        KafkaTopicUri kafkaTopicUri = (KafkaTopicUri) topicUri;

        List<PartitionInfo> partitions = executeBackendCallWithRetriesAndReturn(
                () -> kafkaProducer.partitionsFor(kafkaTopicUri.getTopic()),
                activeTopicUrisOrPartitions.put(topicUri)
        );
        return partitions == null ? null : partitions.stream().map(partitionInfo -> {
            TopicUriPartition topicUriPartition =
                    new TopicUriPartition(kafkaTopicUri.getTopicUriAsString(), partitionInfo.partition());
            BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, kafkaTopicUri);
            return topicUriPartition;
        }).collect(Collectors.toSet());
    }

    @Override
    public Future<MessageId> send(PscProducerMessage<K, V> pscProducerMessage, Callback callback) throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("send()");

        // Reset Kafka producer instance if SSL cert expiry time is approaching
        maybeResetBackendClient(pscProducerMessage.getTopicUriPartition());

        KafkaTopicUri kafkaTopicUri = (KafkaTopicUri) pscProducerMessage.getTopicUriPartition().getTopicUri();
        backendTopicToTopicUri.put(kafkaTopicUri.getTopic(), kafkaTopicUri);

        PscProducerMessage<byte[], byte[]> rawPscProducerMessage = producerInterceptors.onSend(pscProducerMessage);
        Headers headers = null;
        if (rawPscProducerMessage.getHeaders() != null) {
            headers = new RecordHeaders();
            rawPscProducerMessage.getHeaders().forEach(headers::add);
        }

        long produceTimestamp =
                rawPscProducerMessage.getHeaders() != null &&
                        rawPscProducerMessage.getHeaders().containsKey(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP) ?
                        PscCommon.byteArrayToLong(rawPscProducerMessage.getHeader(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP)) :
                        -1;

        ProducerRecord<byte[], byte[]> kafkaProducerRecord = getKafkaProducerRecord(
                kafkaTopicUri.getTopic(),
                rawPscProducerMessage.getPartition(),
                rawPscProducerMessage.getPublishTimestamp(),
                rawPscProducerMessage.getKey(),
                rawPscProducerMessage.getValue(),
                headers
        );

        Future<RecordMetadata> sendResultFuture;

        if (autoResolutionEnabled) {
            final int[] retries = {0};
            final Class[] exception = {null};
            AtomicReference<Future<RecordMetadata>> kafkaFuture = new AtomicReference<>();
            boolean[] futureReady = {false};

            try {
                sendResultFuture = internalSendWithAutoResolution(
                        kafkaProducerRecord, callback, kafkaTopicUri, produceTimestamp,
                        exception, retries, kafkaFuture, futureReady
                );
            } catch (InterruptedException e) {
                throw new ProducerException(e);
            }

            int[] activeProducerCount = {0};
            allProducers.forEach((key, value) -> {
                if (!value) {
                    if (references.get(key) == 0) {
                        key.close();
                        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                kafkaTopicUri,
                                kafkaProducerRecord.partition() == null ? PscUtils.NO_PARTITION : kafkaProducerRecord.partition(),
                                PscMetrics.PSC_PRODUCER_BACKEND_DECOMMISSIONED_COUNT, pscConfigurationInternal
                        );
                        allProducers.remove(key);
                        references.remove(key);
                    } else if (references.get(key) < 0) {
                        logger.error("Reference is negative: " + references.get(key));
                    }
                } else
                    ++activeProducerCount[0];
            });

            if (activeProducerCount[0] > 1) {
                if (potentialLeak.compareAndSet(false, true))
                    logger.info("Active producers bumped to " + activeProducerCount[0]);
            } else {
                if (potentialLeak.compareAndSet(true, false))
                    logger.info("Active producers dropped to " + activeProducerCount[0]);
            }
        } else {
            sendResultFuture = internalSendWithoutAutoResolution(
                    kafkaProducerRecord, callback, kafkaTopicUri, produceTimestamp
            );
        }

        return new Future<MessageId>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return sendResultFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return sendResultFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return sendResultFuture.isDone();
            }

            @Override
            public MessageId get() throws InterruptedException, ExecutionException {
                RecordMetadata kafkaRecordMetadata = sendResultFuture.get();
                return getKafkaMessageId(pscProducerMessage, kafkaRecordMetadata);
            }

            @Override
            public MessageId get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                RecordMetadata kafkaRecordMetadata = sendResultFuture.get(timeout, unit);
                return getKafkaMessageId(pscProducerMessage, kafkaRecordMetadata);
            }

            private MessageId getKafkaMessageId(PscProducerMessage<K, V> inputPscProducerMessage, RecordMetadata outputKafkaRecordMetadata) {
                TopicUriPartition topicUriPartition = new TopicUriPartition(
                        inputPscProducerMessage.getTopicUriPartition().getTopicUriAsString(),
                        outputKafkaRecordMetadata.partition()
                );
                BaseTopicUri.finalizeTopicUriPartition(
                        topicUriPartition, inputPscProducerMessage.getTopicUriPartition().getTopicUri()
                );
                return new KafkaMessageId(
                        topicUriPartition,
                        outputKafkaRecordMetadata.offset(),
                        outputKafkaRecordMetadata.timestamp(),
                        outputKafkaRecordMetadata.serializedKeySize(),
                        outputKafkaRecordMetadata.serializedValueSize()
                );
            }
        };
    }

    private Future<RecordMetadata> internalSendWithAutoResolution(
            ProducerRecord<byte[], byte[]> kafkaProducerRecord,
            Callback callback,
            KafkaTopicUri kafkaTopicUri,
            long produceTimestamp,
            Class[] exceptionToThrow,
            int[] retries,
            AtomicReference<Future<RecordMetadata>> kafkaFuture,
            boolean[] futureReady
    ) throws InterruptedException {
        final KafkaProducer<byte[], byte[]> producer = getProducer();

        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                kafkaTopicUri,
                kafkaProducerRecord.partition() == null ? PscUtils.NO_PARTITION : kafkaProducerRecord.partition(),
                PscMetrics.PSC_PRODUCER_BACKEND_SEND_ATTEMPT_COUNT, pscConfigurationInternal
        );

        kafkaFuture.compareAndSet(null,
                producer.send(kafkaProducerRecord, new org.apache.kafka.clients.producer.Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                        // callback received
                        if (recordMetadata != null)
                            processCallbackInternally(kafkaTopicUri, produceTimestamp, recordMetadata);
                        try {
                            if (exception == null) {
                                String exceptionClassname = exceptionToThrow[0] == null ? "" : "." + exceptionToThrow[0].getName();
                                exceptionToThrow[0] = null;
                                futureReady[0] = true;
                                // successful send
                                if (retries[0] > 0) {
                                    retries[0] = 0;
                                    PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                            kafkaTopicUri, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_SUCCESS + exceptionClassname, pscConfigurationInternal
                                    );
                                }

                                if (callback != null)
                                    callback.onCompletion(getKafkaMessageId(kafkaTopicUri, recordMetadata), null);
                            } else {
                                if (exceptionToThrow[0] == exception.getClass()) {
                                    PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                            kafkaTopicUri, PscMetrics.PSC_PRODUCER_AUTO_RESOLUTION_RETRY_FAILURE + "." + exception.getClass().getName(), pscConfigurationInternal
                                    );
                                } else {
                                    exceptionToThrow[0] = exception.getClass();
                                }

                                PscErrorHandler.ProducerAction action = KafkaErrors.shouldHandleProducerException(exception, autoResolutionEnabled);
                                switch (action.actionType) {
                                    case NONE:
                                    case THROW:
                                        exceptionToThrow[0] =
                                                action.actionType == PscErrorHandler.ActionType.NONE ? null : exception.getClass();
                                        futureReady[0] = true;
                                        retries[0] = 0;
                                        if (callback != null)
                                            callback.onCompletion(
                                                    getKafkaMessageId(kafkaTopicUri, recordMetadata),
                                                    action.actionType == PscErrorHandler.ActionType.NONE ? null : exception
                                            );
                                        break;
                                    case RETRY_THEN_THROW:
                                    case RESET_THEN_THROW:
                                        retries[0] = backoff(retries[0]);
                                        if (retries[0] <= autoResolutionRetryCount) {
                                            // one more exception - of type `exception.getClass()`
                                            if (action.actionType == PscErrorHandler.ActionType.RESET_THEN_THROW)
                                                updateStatus(producer, false);
                                            PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                                    kafkaTopicUri, PscMetrics.PSC_PRODUCER_RETRIES_METRIC, pscConfigurationInternal
                                            );
                                            internalSendWithAutoResolution(kafkaProducerRecord, callback, kafkaTopicUri, produceTimestamp,
                                                    exceptionToThrow, retries, kafkaFuture, futureReady);
                                        } else {
                                            futureReady[0] = true;
                                            // retries reached limit
                                            PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                                                    kafkaTopicUri, PscMetrics.PSC_PRODUCER_RETRIES_REACHED_LIMIT_METRIC, pscConfigurationInternal
                                            );
                                            retries[0] = 0;
                                            if (callback != null)
                                                callback.onCompletion(getKafkaMessageId(kafkaTopicUri, recordMetadata), exception);
                                        }
                                        break;
                                }
                            }
                        } catch (Exception exception1) {
                            futureReady[0] = true;
                            exceptionToThrow[0] = exception1.getClass();
                            if (callback != null)
                                callback.onCompletion(getKafkaMessageId(kafkaTopicUri, recordMetadata), exception1);
                            logger.error("Exception occurred in send callback: ", exception1);
                            //totalExceptionsInCallbacks.incrementAndGet();
                            try {
                                handleException(exception1, kafkaTopicUri, true);
                            } catch (ProducerException producerException) {
                                throw new RuntimeException(producerException);
                            }
                        } finally {
                            references.computeIfPresent(producer, (p, ref) -> --ref);
                        }
                    }
                })
        );
        return kafkaFuture.get();
    }

    private Future<RecordMetadata> internalSendWithoutAutoResolution(
            ProducerRecord<byte[], byte[]> kafkaProducerRecord,
            Callback callback,
            KafkaTopicUri kafkaTopicUri,
            long produceTimestamp
    ) throws ProducerException {
        if (callback == null) {
            return executeBackendCallWithRetriesAndReturn(
                    () -> kafkaProducer.send(kafkaProducerRecord, new org.apache.kafka.clients.producer.Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (recordMetadata != null)
                                processCallbackInternally(kafkaTopicUri, produceTimestamp, recordMetadata);
                        }
                    }),
                    activeTopicUrisOrPartitions.put(kafkaTopicUri)
            );
        } else {
            return executeBackendCallWithRetriesAndReturn(
                    () -> kafkaProducer.send(kafkaProducerRecord, new org.apache.kafka.clients.producer.Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (recordMetadata == null) {
                                callback.onCompletion(null, e == null ? null : new ProducerException(e));
                            } else {
                                try {
                                    processCallbackInternally(kafkaTopicUri, produceTimestamp, recordMetadata);
                                    TopicUriPartition topicUriPartition = new TopicUriPartition(kafkaTopicUri.getTopicUriAsString(), recordMetadata.partition());
                                    KafkaMessageId kafkaMessageId = getKafkaMessageId(topicUriPartition, kafkaTopicUri, recordMetadata);
                                    callback.onCompletion(kafkaMessageId, e == null ? null : new ProducerException(e));
                                } catch (Exception exception) {
                                    try {
                                        handleException(exception, kafkaTopicUri, true);
                                    } catch (ProducerException ex) {
                                        throw new RuntimeException(ex);
                                    }
                                }
                            }
                        }
                    }),
                    activeTopicUrisOrPartitions.put(kafkaTopicUri)
            );
        }
    }

    private KafkaMessageId getKafkaMessageId(KafkaTopicUri kafkaTopicUri, RecordMetadata recordMetadata) {
        if (recordMetadata == null)
            return null;

        TopicUriPartition topicUriPartition = new TopicUriPartition(kafkaTopicUri.getTopicUriAsString(), recordMetadata.partition());
        return getKafkaMessageId(topicUriPartition, kafkaTopicUri, recordMetadata);
    }

    private KafkaMessageId getKafkaMessageId(TopicUriPartition kafkaTopicUriPartition, TopicUri topicUri, RecordMetadata recordMetadata) {
        if (recordMetadata == null)
            return null;

        BaseTopicUri.finalizeTopicUriPartition(kafkaTopicUriPartition, topicUri);
        return new KafkaMessageId(
                kafkaTopicUriPartition,
                recordMetadata.offset(),
                recordMetadata.timestamp(),
                recordMetadata.serializedKeySize(),
                recordMetadata.serializedValueSize()
        );
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicUriPartition, Long> offsetByTopicUriPartition, String consumerGroupId) throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("sendOffsetsToTransaction()");

        offsetByTopicUriPartition.keySet().forEach(topicUriPartition ->
                backendTopicToTopicUri.put(topicUriPartition.getTopicUri().getTopic(), topicUriPartition.getTopicUri())
        );

        executeBackendCallWithRetries(
                () -> kafkaProducer.sendOffsetsToTransaction(
                        offsetByTopicUriPartition.entrySet().stream().collect(
                                Collectors.toMap(
                                        e -> new TopicPartition(e.getKey().getTopicUri().getTopic(), e.getKey().getPartition()),
                                        e -> new OffsetAndMetadata(e.getValue())
                                )
                        ),
                        consumerGroupId
                ),
                activeTopicUrisOrPartitions.put(offsetByTopicUriPartition.keySet())
        );
    }

    /**
     * Updates backend histogram and counter metric and logs send message information for data transfer cost calculations
     * @param kafkaTopicUri the topicUri the message was sent to
     * @param produceTimestamp the producer timestamp from the message header
     * @param recordMetadata the metadata returned from the send call
     */
    private void processCallbackInternally(KafkaTopicUri kafkaTopicUri, long produceTimestamp, RecordMetadata recordMetadata) {
        if (produceTimestamp != -1) {
            PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                    kafkaTopicUri,
                    recordMetadata.partition(),
                    PscMetrics.PSC_PRODUCER_PRODUCE_TIME_MS_METRIC,
                    System.currentTimeMillis() - produceTimestamp, pscConfigurationInternal
            );
        }
        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                kafkaTopicUri, recordMetadata.partition(), PscMetrics.PSC_PRODUCER_ACKED_MESSAGES_METRIC, pscConfigurationInternal
        );
        /*
        chargebackLogger.info("{\"type\": \"producer\", \"backend\": \"kafka\", \"locality\": \"{}\", " +
                        "\"hostname\": \"{}\", \"topicUri\": \"{}\", \"cluster\": \"{}\", \"topic\": \"{}\", " +
                        "\"partition\": {}, \"messageSize\": {}, \"project\": \"{}\", \"timestamp\": {}}",
                environment.getLocality(), environment.getHostname(), kafkaTopicUri, kafkaTopicUri.getCluster(),
                recordMetadata.topic(), recordMetadata.partition(),
                recordMetadata.serializedKeySize() + recordMetadata.serializedValueSize(), project,
                System.currentTimeMillis());
         */
    }

    private ProducerRecord<byte[], byte[]> getKafkaProducerRecord(
            String topic, int partition, long publishTimestamp, byte[] key, byte[] value, Headers headers
    ) {
        return new ProducerRecord<>(
                topic,
                partition == PscUtils.NO_PARTITION ? null : partition,
                publishTimestamp,
                key,
                value,
                headers
        );
    }

    @Override
    public void flush() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("flush()");
        executeBackendCallWithRetries(() -> kafkaProducer.flush());
    }

    @Override
    public void close() throws ProducerException {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void close(Duration duration) throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("close()");
        executeBackendCallWithRetries(() -> kafkaProducer.close(duration));
        super.close(duration);
    }

    @Override
    public PscConfiguration getConfiguration() {
        PscConfiguration pscConfiguration = new PscConfiguration();
        properties.forEach((key, value) -> pscConfiguration.setProperty(
                key.toString(),
                value.toString()
        ));
        return pscConfiguration;
    }

    @Override
    public void abortTransaction() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("abortTransaction()");

        executeBackendCallWithRetries(() -> kafkaProducer.abortTransaction());
    }

    @Override
    public void beginTransaction() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("beginTransaction()");

        executeBackendCallWithRetries(() -> kafkaProducer.beginTransaction());
    }

    @Override
    public void commitTransaction() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("commitTransaction()");

        executeBackendCallWithRetries(() -> kafkaProducer.commitTransaction());
    }

    @Override
    public void initTransaction() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("initTransaction()");

        executeBackendCallWithRetries(() -> kafkaProducer.initTransactions());
    }

    @Override
    public Object getTransactionManager() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("getTransactionManager()");

        try {
            return PscCommon.getField(kafkaProducer, "transactionManager");
        } catch (RuntimeException exception) {
            handleException(exception, true);
            return null;
        }
    }

    private Object getSender() throws ProducerException {
        try {
            return PscCommon.getField(kafkaProducer, "sender");
        } catch (RuntimeException exception) {
            handleException(exception, true);
            return null;
        }
    }

    @Override
    public void wakeup() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("wakeup()");

        try {
            PscCommon.invoke(getSender(), "wakeup");
        } catch (Exception exception) {
            handleException(exception, true);
        }
    }

    @Override
    public void resumeTransaction(PscBackendProducer otherBackendProducer) throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("resumeTransaction()");

        if (!(otherBackendProducer instanceof PscKafkaProducer)) {
            handleException(
                    new BackendProducerException(
                            "[Kafka] Unexpected backend producer type: " + otherBackendProducer.getClass().getCanonicalName(),
                            PscUtils.BACKEND_TYPE_KAFKA
                    ), true
            );
        }

        PscKafkaProducer otherKafkaProducer = (PscKafkaProducer) otherBackendProducer;

        long producerId = otherKafkaProducer.getProducerId();
        short epoch = otherKafkaProducer.getEpoch();

        if (producerId < 0 || epoch < 0) {
            handleException(
                    new BackendProducerException(
                            String.format("Invalid values for producerId %s and epoch %s", producerId, epoch),
                            PscUtils.BACKEND_TYPE_KAFKA
                    ), true
            );
        }

        resumeTransaction(new PscProducerTransactionalProperties(producerId, epoch));
    }

    @Override
    public void resumeTransaction(PscProducerTransactionalProperties pscProducerTransactionalProperties) throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("resumeTransaction()");

        try {
            Object transactionManager = getTransactionManager();
            if (transactionManager == null)
                handleNullTransactionManager();
            synchronized (kafkaProducer) {
                TransactionManagerUtils.resumeTransaction(transactionManager, pscProducerTransactionalProperties);
            }
        } catch (Exception exception) {
            handleException(exception, true);
        }
    }

    @Override
    public PscProducerTransactionalProperties getTransactionalProperties() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("getTransactionalProperties()");

        Object transactionManager = getTransactionManager();
        if (transactionManager == null)
            handleNullTransactionManager();
        return new PscProducerTransactionalProperties(
                TransactionManagerUtils.getProducerId(transactionManager),
                TransactionManagerUtils.getEpoch(transactionManager)
        );
    }

    private void handleNullTransactionManager() throws ProducerException {
        handleException(
                new BackendProducerException(
                        "Attempting to get transactionManager in KafkaProducer when " +
                        "transactionManager is null. This indicates that the KafkaProducer " +
                        "was not initialized to be transaction-ready.",
                        PscUtils.BACKEND_TYPE_KAFKA
                ), true
        );
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() throws ProducerException {
        if (kafkaProducer == null)
            handleUninitializedKafkaProducer("metrics()");
        reportProducerMetrics();
        return metricValueProvider.getMetrics();
    }

    private long getProducerId() {
        try {
            Object transactionManager = getTransactionManager();
            if (transactionManager == null)
                handleNullTransactionManager();
            return TransactionManagerUtils.getProducerId(transactionManager);
        } catch (ProducerException e) {
            throw new RuntimeException("Unable to get producerId", e);
        }
    }

    private void setProducerId(long producerId) {
        try {
            Object transactionManager = getTransactionManager();
            if (transactionManager == null)
                handleNullTransactionManager();
            TransactionManagerUtils.setProducerId(transactionManager, producerId);
        } catch (ProducerException e) {
            throw new RuntimeException("Unable to set producerId", e);
        }
    }

    public short getEpoch() {
        try {
            Object transactionManager = getTransactionManager();
            if (transactionManager == null)
                handleNullTransactionManager();
            return TransactionManagerUtils.getEpoch(transactionManager);
        } catch (ProducerException e) {
            throw new RuntimeException("Unable to get epoch", e);
        }
    }

    private void setEpoch(short epoch) {
        try {
            Object transactionManager = getTransactionManager();
            if (transactionManager == null)
                handleNullTransactionManager();
            TransactionManagerUtils.setEpoch(transactionManager, epoch);
        } catch (ProducerException e) {
            throw new RuntimeException("Unable to set epoch", e);
        }
    }

    @VisibleForTesting
    protected boolean isSslEnabled(TopicUriPartition topicUriPartition) {
        // check if topicUriPartition is using SSL
        return topicUriPartition.getTopicUri().getProtocol().equals(KafkaTopicUri.SECURE_PROTOCOL);
    }

    protected void maybeResetBackendClient(TopicUriPartition topicUriPartition) throws ProducerException {
        // reset if SSL enabled && cert is expired
        if (!pscConfigurationInternal.isProactiveSslResetEnabled()) {
            logger.info("Skipping reset of client even though SSL certificate is approaching expiry at {}" +
                    " because proactive reset is disabled", sslCertificateExpiryTimeInMillis);
            return;
        }
        if (isSslEnabled(topicUriPartition) &&
                (System.currentTimeMillis() >= sslCertificateExpiryTimeInMillis)) {
            if (KafkaSslUtils.keyStoresExist(properties)) {
                logger.info("Resetting backend Kafka client due to cert expiry at " +
                        sslCertificateExpiryTimeInMillis);
                resetBackendClient();

                sslCertificateExpiryTimeInMillis = KafkaSslUtils.calculateSslCertExpiryTime(
                        properties, pscConfigurationInternal, Collections.singleton(topicUriPartition));
            } else {
                logger.warn("Cannot verify existence of key store and trust store. " +
                        "Continue to run with old, expired certificate with expiry time " +
                        sslCertificateExpiryTimeInMillis);
            }
        }
    }

    @Override
    protected void resetBackendClient() throws ProducerException {
        super.resetBackendClient();
        logger.warn("Resetting the backend Kafka producer (potentially to retry an API if an earlier call failed).");
        executeBackendCallWithRetries(() -> kafkaProducer.close());
        kafkaProducer = new KafkaProducer<>(properties);
    }

    private void handleException(Exception exception, boolean emitMetrics) throws ProducerException {
        handleException(exception, (Set) null, emitMetrics);
    }

    private void handleException(Exception exception, TopicUri topicUri, boolean emitMetrics) throws ProducerException {
        handleException(exception, Collections.singleton(topicUri), emitMetrics);
    }

    private void handleException(Exception exception, TopicUriPartition topicUriPartition, boolean emitMetrics) throws ProducerException {
        handleException(exception, Collections.singleton(topicUriPartition), emitMetrics);
    }

    private void handleUninitializedKafkaProducer(String callerMethod) throws ProducerException {
        handleException(
                new BackendProducerException(
                        String.format("[Kafka] Producer is not initialized prior to call to %s.", callerMethod),
                        PscUtils.BACKEND_TYPE_KAFKA
                ), true
        );
    }
}
