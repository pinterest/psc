package com.pinterest.psc.consumer.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaErrors;
import com.pinterest.psc.common.kafka.KafkaMessageId;
import com.pinterest.psc.common.kafka.KafkaSslUtils;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConsumerToKafkaConsumerConfigConverter;
import com.pinterest.psc.consumer.ConsumerRebalanceListener;
import com.pinterest.psc.consumer.OffsetCommitCallback;
import com.pinterest.psc.consumer.PscBackendConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.BackendConsumerException;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.metrics.kafka.KafkaMetricsHandler;
import com.pinterest.psc.metrics.kafka.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PscKafkaConsumer<K, V> extends PscBackendConsumer<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(PscKafkaConsumer.class);
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final Set<TopicUri> currentSubscription = new HashSet<>();
    private final Set<TopicUriPartition> currentAssignment = new HashSet<>();
    private long kafkaPollTimeoutMs;
    private Properties properties;
    private final AtomicReference<Boolean> resetOnNextInvocation = new AtomicReference(false);
    private long sslCertificateExpiryTimeInMillis;
    private boolean isSslEnabledInAnyActiveSusbcriptionOrAssignment = false;

    @Override
    public void initializeBackend(ServiceDiscoveryConfig discoveryConfig, TopicUri topicUri) {
        properties = new PscConsumerToKafkaConsumerConfigConverter().convert(pscConfigurationInternal, topicUri);
        properties.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                discoveryConfig.getConnect()
        );
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName()
        );
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName()
        );
        properties.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG,
                pscConfigurationInternal.getPscConsumerClientId() + "-" + UUID.randomUUID()
        );

        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaPollTimeoutMs = pscConfigurationInternal.getPscConsumerPollTimeoutMs();

        // if using secure protocol (SSL), calculate cert expiry time
        if (topicUri.getProtocol().equals(KafkaTopicUri.SECURE_PROTOCOL)) {
            isSslEnabledInAnyActiveSusbcriptionOrAssignment = true;
            sslCertificateExpiryTimeInMillis = KafkaSslUtils.calculateSslCertExpiryTime(
                    properties, pscConfigurationInternal, Collections.singleton(topicUri));
            logger.info("Initialized PscKafkaConsumer with SSL cert expiry time at " + sslCertificateExpiryTimeInMillis);
        }
        logger.info("Proactive SSL reset enabled: {}", pscConfigurationInternal.isProactiveSslResetEnabled());
    }

    @Override
    protected synchronized void reportConsumerMetrics() {
        if (kafkaConsumer == null)
            return;

        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> kafkaMetrics = kafkaConsumer.metrics();
        KafkaMetricsHandler.handleKafkaClientMetrics(kafkaMetrics, backendTopicToTopicUri, false, pscConfigurationInternal);
        KafkaUtils.convertKafkaMetricsToPscMetrics(kafkaMetrics, metricValueProvider);
    }

    public PscKafkaConsumer() {
    }

    public void subscribe(Set<TopicUri> topicUris, ConsumerRebalanceListener consumerRebalanceListener) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("subscribe()");

        if (!currentAssignment.isEmpty()) {
            handleException(
                    new BackendConsumerException(
                            "[Kafka] Consumer subscribe() is not supported when consumer is already assigned to partitions.",
                            PscUtils.BACKEND_TYPE_KAFKA
                    ), true
            );
        }

        if (topicUris.isEmpty()) {
            unsubscribe();
            return;
        }

        Set<String> topics = new HashSet<>();
        for (TopicUri topicUri : topicUris) {
            if (!(topicUri instanceof KafkaTopicUri)) {
                handleException(
                        new BackendConsumerException(
                                "[Kafka] " + topicUri + " is not a valid Kafka URI",
                                PscUtils.BACKEND_TYPE_KAFKA
                        ), true
                );
            }
            String topic = topicUri.getTopic();
            topics.add(topic);
            backendTopicToTopicUri.put(topic, topicUri);
        }
        // update the subscription list with the new topic uris
        currentSubscription.retainAll(topicUris);
        currentSubscription.addAll(topicUris);
        if (consumerRebalanceListener == null) {
            executeBackendCallWithRetries(() -> kafkaConsumer.subscribe(topics),
                activeTopicUrisOrPartitions.put(topicUris));
        } else {
            executeBackendCallWithRetries(
                () -> kafkaConsumer.subscribe(topics,
                    new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            Set<TopicUriPartition> topicUriPartitions = new HashSet<>();
                            partitions.forEach((topicPartition -> {
                                TopicUri
                                    topicUri =
                                    backendTopicToTopicUri.get(topicPartition.topic());
                                TopicUriPartition topicUriPartition = new TopicUriPartition(
                                    topicUri.getTopicUriAsString(), topicPartition.partition()
                                );
                                BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
                                topicUriPartitions.add(topicUriPartition);
                            }));
                            consumerRebalanceListener.onPartitionsRevoked(topicUriPartitions);
                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            Set<TopicUriPartition> topicUriPartitions = new HashSet<>();
                            partitions.forEach((topicPartition -> {
                                TopicUri
                                    topicUri =
                                    backendTopicToTopicUri.get(topicPartition.topic());
                                TopicUriPartition topicUriPartition = new TopicUriPartition(
                                    topicUri.getTopicUriAsString(), topicPartition.partition()
                                );
                                BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
                                topicUriPartitions.add(topicUriPartition);
                            }));
                            consumerRebalanceListener.onPartitionsAssigned(topicUriPartitions);
                        }
                    }),
                activeTopicUrisOrPartitions.put(topicUris)
            );
        }
        isSslEnabledInAnyActiveSusbcriptionOrAssignment = detectIfSslEnabledInActiveAssignmentOrSubscription();
    }

    @Override
    public void subscribe(Set<TopicUri> topicUris) throws ConsumerException {
        subscribe(topicUris, null);
    }

    @Override
    public Set<TopicUri> subscription() throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("subscription()");

        return currentSubscription;
    }

    @Override
    public void unsubscribe() throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("unsubscribe()");

        kafkaConsumer.unsubscribe();
        currentSubscription.clear();
        isSslEnabledInAnyActiveSusbcriptionOrAssignment = detectIfSslEnabledInActiveAssignmentOrSubscription();
    }

    @Override
    public void assign(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("assign()");

        if (!currentSubscription.isEmpty()) {
            handleException(
                    new BackendConsumerException(
                            "[Kafka] Consumer assign() is not supported when consumer is already subscribed to topics.",
                            PscUtils.BACKEND_TYPE_KAFKA
                    ), true
            );
        }

        if (topicUriPartitions.isEmpty()) {
            unsubscribe();
            return;
        }

        for (TopicUriPartition topicUriPartition : topicUriPartitions)
            backendTopicToTopicUri.put(topicUriPartition.getTopicUri().getTopic(), topicUriPartition.getTopicUri());

        // update the assignment list with the new topic uris
        executeBackendCallWithRetries(
                () -> kafkaConsumer.assign(
                        topicUriPartitions.stream().map(topicUriPartition ->
                                new TopicPartition(
                                        topicUriPartition.getTopicUri().getTopic(),
                                        topicUriPartition.getPartition()
                                )
                        ).collect(Collectors.toList())
                ),
                topicUriPartitions
        );
        currentAssignment.retainAll(topicUriPartitions);
        currentAssignment.addAll(topicUriPartitions);
        isSslEnabledInAnyActiveSusbcriptionOrAssignment = detectIfSslEnabledInActiveAssignmentOrSubscription();
    }

    @Override
    public void unassign() throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("unassign()");

        kafkaConsumer.unsubscribe();
        currentAssignment.clear();
        isSslEnabledInAnyActiveSusbcriptionOrAssignment = detectIfSslEnabledInActiveAssignmentOrSubscription();
    }

    @Override
    public Set<TopicUriPartition> assignment() throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("assignment()");

        if (!currentSubscription.isEmpty()) {
            Set<TopicPartition> kafkaConsumerAssignment = executeBackendCallWithRetriesAndReturn(
                    () -> kafkaConsumer.assignment()
            );
            return kafkaConsumerAssignment.stream().map(topicPartition -> {
                TopicUri topicUri = backendTopicToTopicUri.get(topicPartition.topic());
                TopicUriPartition topicUriPartition =
                        new TopicUriPartition(topicUri.getTopicUriAsString(), topicPartition.partition());
                BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
                return topicUriPartition;
            }).collect(Collectors.toSet());
        }

        return currentAssignment;
    }

    @Override
    public PscConsumerPollMessageIterator<K, V> poll(Duration pollTimeout) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("poll()");

        long startTs = System.currentTimeMillis();

        // Reset Kafka consumer instance if SSL cert expiry time is approaching
        maybeResetBackendClient();

        ConsumerRecords<byte[], byte[]> records = executeBackendCallWithRetriesAndReturn(
                () -> kafkaConsumer.poll(pollTimeout), getActiveTopicUrisOrPartitions()
        );
        long stopTs = System.currentTimeMillis();

        if (!this.currentSubscription.isEmpty()) {
            for (TopicUri topicUri : this.currentSubscription) {
                PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                        topicUri, PscMetrics.PSC_CONSUMER_POLL_TIME_MS_METRIC, stopTs - startTs,
                        pscConfigurationInternal);
            }
        } else if (!this.currentAssignment.isEmpty()) {
            for (TopicUriPartition topicUriPartition : this.currentAssignment) {
                PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                        topicUriPartition.getTopicUri(),
                        topicUriPartition.getPartition(),
                        PscMetrics.PSC_CONSUMER_POLL_TIME_MS_METRIC,
                        stopTs - startTs,
                        pscConfigurationInternal);
            }
        }

        Set<TopicPartition> topicPartitions = records.partitions();
        Map<TopicPartition, Iterator<ConsumerRecord<byte[], byte[]>>> perPartitionIterators = new HashMap<>(topicPartitions.size());
        for (TopicPartition topicPartition : topicPartitions)
            perPartitionIterators.put(topicPartition, records.records(topicPartition).iterator());

        return new KafkaToPscMessageIteratorConverter<>(
                records.iterator(),
                perPartitionIterators,
                topicPartitions,
                backendTopicToTopicUri,
                getConsumerInterceptors()
        );
    }

    private Map<TopicPartition, OffsetAndMetadata> getMaxOffsetByTopicPartition(Set<MessageId> messageIds) throws ConsumerException {
        Map<TopicPartition, OffsetAndMetadata> maxOffsets = new HashMap<>();
        for (MessageId messageId : messageIds) {
            TopicUri topicUri = messageId.getTopicUriPartition().getTopicUri();
            if (!(topicUri instanceof KafkaTopicUri)) {
                handleException(
                        new BackendConsumerException(
                                "[Kafka] Message id with illegal topic URI " + topicUri + " was passed to commit().",
                                PscUtils.BACKEND_TYPE_KAFKA
                        ), true
                );
            }

            KafkaMessageId kafkaMessageId = getKafkaMessageId(messageId);

            TopicPartition topicPartition = new TopicPartition(
                    topicUri.getTopic(),
                    kafkaMessageId.getTopicUriPartition().getPartition()
            );
            // The last committed offset is the next offset to consume, hence adding 1 to consumed offset
            long offset = kafkaMessageId.getOffset() + 1;
            maxOffsets.compute(topicPartition,
                    (key, val) -> (val == null) ? new OffsetAndMetadata(offset) :
                            (offset > val.offset()) ? new OffsetAndMetadata(offset) : val);
        }

        return maxOffsets;
    }

    private void internalCommitAsyncWithoutAutoResolution(OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("commitAsync()");

        if (offsetCommitCallback == null)
            executeBackendCallWithRetries(
                    () -> kafkaConsumer.commitAsync(),
                    getActiveTopicUrisOrPartitions()
            );
        else {
            executeBackendCallWithRetries(
                    () -> kafkaConsumer.commitAsync(new org.apache.kafka.clients.consumer.OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                            Map<TopicUriPartition, MessageId> pscOffsets = new HashMap<>(offsets.size());
                            offsets.forEach(((topicPartition, offsetAndMetadata) -> {
                                TopicUri topicUri = backendTopicToTopicUri.get(topicPartition.topic());
                                TopicUriPartition topicUriPartition = new TopicUriPartition(
                                        topicUri.getTopicUriAsString(), topicPartition.partition()
                                );
                                BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
                                pscOffsets.put(topicUriPartition, new KafkaMessageId(topicUriPartition, offsetAndMetadata.offset()));
                            }));

                            offsetCommitCallback.onCompletion(pscOffsets, e == null ? null : new ConsumerException(e));
                            if (e != null) {
                                getConsumerInterceptors().onCommit(new HashSet<>(pscOffsets.values()));
                            }
                        }
                    }),
                    getActiveTopicUrisOrPartitions()
            );
        }
    }

    private void internalCommitAsyncWithoutAutoResolution(Set<MessageId> messageIds, OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("commitAsync()");

        Map<TopicPartition, OffsetAndMetadata> maxOffsetByTopicPartition = getMaxOffsetByTopicPartition(messageIds);
        if (offsetCommitCallback == null) {
            executeBackendCallWithRetries(
                    () -> kafkaConsumer.commitAsync(maxOffsetByTopicPartition, null),
                    getActiveTopicUrisOrPartitions()
            );
        } else {
            executeBackendCallWithRetries(
                    () -> kafkaConsumer.commitAsync(maxOffsetByTopicPartition, new org.apache.kafka.clients.consumer.OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                            Map<TopicUriPartition, MessageId> pscOffsets = new HashMap<>(offsets.size());
                            offsets.forEach(((topicPartition, offsetAndMetadata) -> {
                                TopicUri topicUri = backendTopicToTopicUri.get(topicPartition.topic());
                                TopicUriPartition topicUriPartition = new TopicUriPartition(
                                        topicUri.getTopicUriAsString(), topicPartition.partition()
                                );
                                BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
                                pscOffsets.put(topicUriPartition, new KafkaMessageId(topicUriPartition, offsetAndMetadata.offset()));
                            }));

                            offsetCommitCallback.onCompletion(pscOffsets, e == null ? null : new ConsumerException(e));
                            // TODO: to verify
                            if (e != null) {
                                getConsumerInterceptors().onCommit(messageIds);
                            }
                        }
                    }),
                    getActiveTopicUrisOrPartitions()
            );
        }
    }

    private void internalCommitAsyncWithAutoResolution(
            Set<MessageId> messageIds,
            OffsetCommitCallback offsetCommitCallback,
            int[] retries,
            Exception[] exceptionToThrow) throws ConsumerException {

        if (kafkaConsumer == null) {
            handleUninitializedKafkaConsumer("commitAsync()");
        }

        Map<TopicPartition, OffsetAndMetadata> maxOffsetByTopicPartition = null;
        if (messageIds != null) {
            maxOffsetByTopicPartition = getMaxOffsetByTopicPartition(messageIds);
        }
        final Map<TopicPartition, OffsetAndMetadata> finalMaxOffsetByTopicPartition = maxOffsetByTopicPartition;
        if (offsetCommitCallback == null) {
            if (finalMaxOffsetByTopicPartition == null) {
                executeBackendCallWithRetries(
                        () -> kafkaConsumer.commitAsync(),
                        getActiveTopicUrisOrPartitions()
                );
            } else {
                executeBackendCallWithRetries(
                        () -> kafkaConsumer.commitAsync(finalMaxOffsetByTopicPartition, null),
                        getActiveTopicUrisOrPartitions()
                );
            }
        } else {
            org.apache.kafka.clients.consumer.OffsetCommitCallback customOffsetCommitCallback = new org.apache.kafka.clients.consumer.OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {

                    Map<TopicUriPartition, MessageId> pscOffsets = new HashMap<>(offsets.size());
                    offsets.forEach(((topicPartition, offsetAndMetadata) -> {
                        TopicUri topicUri = backendTopicToTopicUri.get(topicPartition.topic());
                        TopicUriPartition topicUriPartition = new TopicUriPartition(
                                topicUri.getTopicUriAsString(), topicPartition.partition()
                        );
                        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
                        pscOffsets.put(topicUriPartition, new KafkaMessageId(topicUriPartition, offsetAndMetadata.offset()));
                    }));

                    boolean wasPreviouslyReset = resetOnNextInvocation.getAndSet(false);

                    if (e == null) {
                        String exceptionClassname = exceptionToThrow[0] == null ? "" : exceptionToThrow[0].getClass().getName();
                        if (retries[0] > 0 || wasPreviouslyReset) {
                            logger.info("Successful auto resolution in commitAsync callback for " + exceptionClassname);
                            // successful auto resolution
                            retries[0] = 0;
                            incrementCounterMetricForSubscriptionsOrAssignments(PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS + "." + exceptionClassname);
                        }
                        offsetCommitCallback.onCompletion(pscOffsets, null);
                    } else {
                        logger.warn("commitAsync failed, entering auto-resolution logic attempting to resolve " + e);
                        if (wasPreviouslyReset) {
                            logger.warn("Client was reset but commitAsync still failed", e);
                            incrementCounterMetricForSubscriptionsOrAssignments(PscMetrics.PSC_CONSUMER_AUTO_RESOLUTION_RETRY_FAILURE + "." + e.getClass().getName());
                        }
                        // commit failed, enter auto-resolution logic
                        exceptionToThrow[0] = e;
                        PscErrorHandler.ConsumerAction action = KafkaErrors.shouldHandleConsumerException(e, autoResolutionEnabled);
                        switch (action.actionType) {
                            case NONE:
                            case THROW:
                                exceptionToThrow[0] = action.actionType == PscErrorHandler.ActionType.NONE ? null : e;
                                retries[0] = 0;
                                break;
                            case RESET_THEN_THROW:
                                try {
                                    resetBackendClient();
                                } catch (ConsumerException ex) {
                                    logger.error("Exception occurred in onComplete callback during reset", ex);
                                }
                                break;
                            case RETRY_THEN_THROW:
                            case RETRY_RESET_THEN_THROW:
                                retries[0] = backoff(retries[0]);
                                if (retries[0] <= autoResolutionRetryCount) {
                                    try {
                                        // retry
                                        incrementCounterMetricForSubscriptionsOrAssignments(PscMetrics.PSC_CONSUMER_RETRIES_METRIC);
                                        internalCommitAsyncWithAutoResolution(messageIds, offsetCommitCallback, retries, exceptionToThrow);
                                    } catch (ConsumerException ce) {
                                        exceptionToThrow[0] = ce;
                                        logger.error("Exception occurred in commit onComplete callback during retry: ", ce);
                                        offsetCommitCallback.onCompletion(pscOffsets, exceptionToThrow[0]);
                                    }

                                } else {
                                    // retries reached limit
                                    retries[0] = 0;
                                    String exceptionClassname = e.getClass().getName();
                                    logger.error("Reached limit for retries in commit callback while attempting to resolve " + exceptionClassname);
                                    incrementCounterMetricForSubscriptionsOrAssignments(PscMetrics.PSC_CONSUMER_RETRIES_REACHED_LIMIT_METRIC);
                                    offsetCommitCallback.onCompletion(pscOffsets, exceptionToThrow[0]);
                                    getConsumerInterceptors().onCommit(new HashSet<>(pscOffsets.values()));

                                    if (action.actionType == PscErrorHandler.ActionType.RETRY_RESET_THEN_THROW) {
                                        // last ditch effort - reset on next invocation
                                        logger.warn("ActionType is RETRY_RESET_THEN_THROW, will reset client on next invocation");
                                        resetOnNextInvocation.set(true);
                                    }
                                }
                                break;
                        }
                    }
                }
            };

            // if previous invocation failed
            if (resetOnNextInvocation.get()) {
                // TODO: implement reset with seek to finalMaxOffsetByTopicPartition to prevent rewind
                resetBackendClient();
            }

            if (finalMaxOffsetByTopicPartition == null) {
                kafkaConsumer.commitAsync(customOffsetCommitCallback);
            } else {
                kafkaConsumer.commitAsync(finalMaxOffsetByTopicPartition, customOffsetCommitCallback);
            }
        }
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        if (autoResolutionEnabled) {
            final int[] retries = {0};
            final Exception[] exception = {null};
            internalCommitAsyncWithAutoResolution(null, offsetCommitCallback, retries, exception);
        } else {
            internalCommitAsyncWithoutAutoResolution(offsetCommitCallback);
        }
    }

    @Override
    public void commitAsync(Set<MessageId> messageIds, OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        if (autoResolutionEnabled) {
            final int[] retries = {0};
            final Exception[] exception = {null};
            internalCommitAsyncWithAutoResolution(messageIds, offsetCommitCallback, retries, exception);
        } else {
            internalCommitAsyncWithoutAutoResolution(messageIds, offsetCommitCallback);
        }
    }

    @Override
    public Set<MessageId> commitSync() throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("commitSync()");

        executeBackendCallWithRetries(
                () -> kafkaConsumer.commitSync(),
                getActiveTopicUrisOrPartitions()
        );

        /*
        // using Kafka consumer APIs - leads to several API calls slowing down the execution.
        Set<MessageId> kafkaMessageIds = new HashSet<>();
        kafkaConsumer.assignment().forEach(topicPartition -> {
            long offset = kafkaConsumer.committed(topicPartition).offset();
            TopicUri topicUri = backendTopicToTopicUri.get(topicPartition.topic());
            if (topicUri == null)
                logger.warn("Could not find a registered topic URI for backend topic {}", topicPartition.topic());
            else
                kafkaMessageIds.add(new KafkaMessageId(
                        new TopicUriPartition(topicUri.getTopicUriAsString(), topicPartition.partition()), offset
                ));
        });
         */

        // alternate reflection-based approach using a one-time call - performs ~ 20x faster
        SubscriptionState subscriptions = (SubscriptionState) PscCommon.getField(kafkaConsumer, "subscriptions");
        Map<TopicPartition, OffsetAndMetadata> offsets = subscriptions.allConsumed();
        Set<MessageId> kafkaMessageIds = new HashSet<>();
        if (offsets != null) {
            offsets.forEach(((topicPartition, offsetAndMetadata) -> {
                TopicUri topicUri = backendTopicToTopicUri.get(topicPartition.topic());
                if (topicUri == null)
                    logger.warn("Could not find a registered topic URI for backend topic {}", topicPartition.topic());
                else
                    kafkaMessageIds.add(new KafkaMessageId(
                            new TopicUriPartition(topicUri.getTopicUriAsString(), topicPartition.partition()),
                            offsetAndMetadata.offset()
                    ));
            }));
        }

        return kafkaMessageIds;
    }

    @Override
    public void commitSync(Set<MessageId> messageIds) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("commitSync()");

        Map<TopicPartition, OffsetAndMetadata> maxOffsetByTopicPartition = getMaxOffsetByTopicPartition(messageIds);
        executeBackendCallWithRetries(
                () -> kafkaConsumer.commitSync(maxOffsetByTopicPartition),
                activeTopicUrisOrPartitions.put(maxOffsetByTopicPartition.keySet())
        );
    }

    public void seek(Set<MessageId> messageIds) throws ConsumerException {
        for (MessageId messageId : messageIds)
            seek(messageId);
    }

    public void seek(MessageId messageId) throws ConsumerException {
        KafkaMessageId kafkaMessageId = getKafkaMessageId(messageId);
        seekToOffset(kafkaMessageId.getTopicUriPartition(), messageId.getOffset() + 1);
    }

    @Override
    public void seekToTimestamp(TopicUri topicUri, long timestamp) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("seekToTimestamp()");

        if (!(topicUri instanceof KafkaTopicUri)) {
            handleException(
                    new BackendConsumerException(
                            "[Kafka] Illegal topic URI " + topicUri + " was passed to seek().",
                            PscUtils.BACKEND_TYPE_KAFKA
                    ), true
            );
        }

        if (!subscription().contains(topicUri)) {
            handleException(
                    new BackendConsumerException(
                            "[Kafka] Consumer is not subscribed to topic URI " + topicUri + " passed to seek().",
                            PscUtils.BACKEND_TYPE_KAFKA
                    ), true
            );
        }

        // needed before a call to seek
        Set<TopicPartition> assignment = waitForAssignment();

        KafkaTopicUri kafkaUri = (KafkaTopicUri) topicUri;
        // find which partitions of the given topic uri is assigned to this consumer, to call seek only on those
        List<Integer> assignedPartitionsOfTopicUri = assignment.stream().filter(
                topicPartition -> topicPartition.topic().equals(kafkaUri.getTopic())
        ).map(TopicPartition::partition).collect(Collectors.toList());
        Map<TopicPartition, Long> timestampByTopicPartition = new HashMap<>();
        for (int assignedPartition : assignedPartitionsOfTopicUri)
            timestampByTopicPartition.put(new TopicPartition(kafkaUri.getTopic(), assignedPartition), timestamp);
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimestamp =
                executeBackendCallWithRetriesAndReturn(
                        () -> kafkaConsumer.offsetsForTimes(timestampByTopicPartition),
                        activeTopicUrisOrPartitions.put(topicUri)
                );

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimestamp.entrySet()) {
            if (entry.getValue() == null)
                logger.warn("[Kafka] No offset and timestamp was found for {}", entry.getKey());
            else {
                executeBackendCallWithRetries(
                        () -> kafkaConsumer.seek(entry.getKey(), entry.getValue().offset()),
                        activeTopicUrisOrPartitions.put(topicUri)
                );
            }
        }
    }

    @Override
    public void seekToTimestamp(Map<TopicUriPartition, Long> seekPositions) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("seekToTimestamp()");

        Map<TopicPartition, Long> timestampByTopicPartition = new HashMap<>();
        TopicUriPartition topicUriPartition;
        TopicUri topicUri;
        for (Map.Entry<TopicUriPartition, Long> entry : seekPositions.entrySet()) {
            topicUriPartition = entry.getKey();
            topicUri = topicUriPartition.getTopicUri();
            backendTopicToTopicUri.put(topicUri.getTopic(), topicUri);
            timestampByTopicPartition.put(
                    new TopicPartition(topicUri.getTopic(), topicUriPartition.getPartition()), entry.getValue()
            );
        }

        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimestamp =
                executeBackendCallWithRetriesAndReturn(
                        () -> kafkaConsumer.offsetsForTimes(timestampByTopicPartition),
                        activeTopicUrisOrPartitions.put(seekPositions.keySet())
                );

        // needed before a call to seek
        waitForAssignment();

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimestamp.entrySet()) {
            if (entry.getValue() == null)
                logger.warn("[Kafka] No offset and timestamp was found for {}", entry.getKey());
            else {
                executeBackendCallWithRetries(
                        () -> kafkaConsumer.seek(entry.getKey(), entry.getValue().offset()),
                        activeTopicUrisOrPartitions.put(seekPositions.keySet())
                );
            }
        }
    }

    @Override
    public void seekToOffset(Map<TopicUriPartition, Long> seekPositions) throws ConsumerException {
        for (Map.Entry<TopicUriPartition, Long> entry : seekPositions.entrySet()) {
            seekToOffset(entry.getKey(), entry.getValue());
        }
    }

    private void seekToOffset(TopicUriPartition topicUriPartition, Long offset) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("seekToOffset()");

        TopicUri topicUri = topicUriPartition.getTopicUri();
        if (!(topicUri instanceof KafkaTopicUri)) {
            handleException(
                    new BackendConsumerException(
                            "[Kafka] Message id with illegal topic URI " + topicUri + " was passed to commit().",
                            PscUtils.BACKEND_TYPE_KAFKA
                    ), true
            );
        }
        backendTopicToTopicUri.put(topicUri.getTopic(), topicUri);

        if (!this.currentSubscription.contains(topicUri) &&
                !this.currentAssignment.contains(topicUriPartition)) {
            handleException(
                    new BackendConsumerException(
                            String.format("[Kafka] Consumer is not subscribed to the topicUri '%s' or" +
                                    "assigned the TopicUriPartition '%s' passed to seek().", topicUri, topicUriPartition
                            ),
                            PscUtils.BACKEND_TYPE_KAFKA
                    ), true
            );
        }

        // needed before a call to seek
        waitForAssignment();

        executeBackendCallWithRetries(() ->
                kafkaConsumer.seek(new TopicPartition(topicUri.getTopic(), topicUriPartition.getPartition()), offset),
                activeTopicUrisOrPartitions.put(topicUriPartition)
        );
    }

    @Override
    public void seekToBeginning(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("seekToBeginning()");

        // needed before a call to seek
        waitForAssignment();

        executeBackendCallWithRetries(
                () -> kafkaConsumer.seekToBeginning(topicUriPartitions.stream().map(
                        topicUriPartition -> new TopicPartition(
                                topicUriPartition.getTopicUri().getTopic(),
                                topicUriPartition.getPartition()
                        )
                ).collect(Collectors.toSet())),
                topicUriPartitions
        );
    }

    @Override
    public void seekToEnd(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("seekToEnd()");

        // needed before a call to seek
        waitForAssignment();

        executeBackendCallWithRetries(
                () -> kafkaConsumer.seekToEnd(topicUriPartitions.stream().map(
                        topicUriPartition -> new TopicPartition(
                                topicUriPartition.getTopicUri().getTopic(),
                                topicUriPartition.getPartition()
                        )
                ).collect(Collectors.toSet())),
                topicUriPartitions
        );
    }

    @Override
    public Set<TopicUriPartition> getPartitions(TopicUri topicUri) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("getPartitions()");

        KafkaTopicUri kafkaTopicUri = (KafkaTopicUri) topicUri;

        List<PartitionInfo> partitions = executeBackendCallWithRetriesAndReturn(
                () -> kafkaConsumer.partitionsFor(kafkaTopicUri.getTopic()),
                activeTopicUrisOrPartitions.put(topicUri)
        );
        return partitions == null ? null : partitions.stream().map(partitionInfo -> {
            TopicUriPartition topicUriPartition =
                    new TopicUriPartition(kafkaTopicUri.getTopicUriAsString(), partitionInfo.partition());
            BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, kafkaTopicUri);
            return topicUriPartition;
        }).collect(Collectors.toSet());
    }

    public Set<String> getTopicNames() throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("getTopicNamesInCluster");

        Set<String> topics = new HashSet<>();
        executeBackendCallWithRetriesAndReturn(() -> kafkaConsumer.listTopics()).forEach(
            (topic, partitionInfo) -> topics.add(topic));
        return topics;
    }

    @Override
    public void wakeup() {
        if (kafkaConsumer != null)
            kafkaConsumer.wakeup();
    }

    public void close() throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("close()");

        executeBackendCallWithRetries(() -> kafkaConsumer.close());
        currentSubscription.clear();
        currentAssignment.clear();
        isSslEnabledInAnyActiveSusbcriptionOrAssignment = false;
        // use default for Kafka close timeout
        super.close(Duration.ofMillis(30000L));
    }

    @Override
    public void close(Duration timeout) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("close()");

        executeBackendCallWithRetries(() -> kafkaConsumer.close(timeout));
        currentSubscription.clear();
        currentAssignment.clear();
        isSslEnabledInAnyActiveSusbcriptionOrAssignment = false;
        super.close(timeout);
    }

    @Override
    public MessageId committed(TopicUriPartition topicUriPartition) throws ConsumerException, WakeupException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("committed()");

        KafkaTopicUri kafkaTopicUri = (KafkaTopicUri) topicUriPartition.getTopicUri();

        OffsetAndMetadata offsetAndMetadata;
        try {
            offsetAndMetadata = executeBackendCallWithRetriesAndReturn(() ->
                    kafkaConsumer.committed(
                            new TopicPartition(kafkaTopicUri.getTopic(), topicUriPartition.getPartition())
                    ),
                    activeTopicUrisOrPartitions.put(topicUriPartition)
            );
            return offsetAndMetadata == null ? null : new KafkaMessageId(topicUriPartition, offsetAndMetadata.offset());
        } catch (Exception exception) {
            handleException(exception, true);
            return null;
        }
    }

    @Override
    public Collection<MessageId> committed(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("committed()");

        Set<TopicPartition> topicPartitions = topicUriPartitions.stream().map(tup -> {
            KafkaTopicUri kafkaTopicUri = (KafkaTopicUri) tup.getTopicUri();
            return new TopicPartition(kafkaTopicUri.getTopic(), tup.getPartition());
        }).collect(Collectors.toSet());

        Map<TopicPartition, OffsetAndMetadata> topicPartitionsToOffsetAndMetadata =
                executeBackendCallWithRetriesAndReturn(() ->
                        kafkaConsumer.committed(topicPartitions), activeTopicUrisOrPartitions.put(topicUriPartitions)
                );
        return topicPartitionsToOffsetAndMetadata.entrySet().stream().map(entry -> {
            TopicUriPartition topicUriPartition = new TopicUriPartition(
                    entry.getKey().topic(), entry.getKey().partition()
            );
            return new KafkaMessageId(topicUriPartition, entry.getValue() == null ? null : entry.getValue().offset());
        }).collect(Collectors.toList());
    }

    @Override
    public Map<TopicUriPartition, Long> startOffsets(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("startOffsets()");

        Map<TopicPartition, TopicUriPartition> topicPartitionToTopicUriPartition = new HashMap<>();
        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            topicPartitionToTopicUriPartition.put(
                    new TopicPartition(
                            topicUriPartition.getTopicUri().getTopic(),
                            topicUriPartition.getPartition()
                    ),
                    topicUriPartition
            );
        }

        return executeBackendCallWithRetriesAndReturn(
                () -> kafkaConsumer.beginningOffsets(topicPartitionToTopicUriPartition.keySet()).entrySet().stream().
                        collect(Collectors.toMap(
                                e -> topicPartitionToTopicUriPartition.get(e.getKey()),
                                Map.Entry::getValue
                        )),
                topicUriPartitions
        );
    }

    @Override
    public Map<TopicUriPartition, Long> endOffsets(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("endOffsets()");

        Map<TopicPartition, TopicUriPartition> topicPartitionToTopicUriPartition = new HashMap<>();
        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            topicPartitionToTopicUriPartition.put(
                    new TopicPartition(
                            topicUriPartition.getTopicUri().getTopic(),
                            topicUriPartition.getPartition()
                    ),
                    topicUriPartition
            );
        }

        return executeBackendCallWithRetriesAndReturn(
                () -> kafkaConsumer.endOffsets(topicPartitionToTopicUriPartition.keySet()).entrySet().stream().
                        collect(Collectors.toMap(
                                e -> topicPartitionToTopicUriPartition.get(e.getKey()),
                                Map.Entry::getValue
                        )),
                topicUriPartitions
        );
    }

    @Override
    public long position(TopicUriPartition topicUriPartition) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("position()");

        KafkaTopicUri kafkaTopicUri = (KafkaTopicUri) topicUriPartition.getTopicUri();

        return executeBackendCallWithRetriesAndReturn(
                () -> kafkaConsumer.position(
                        new TopicPartition(kafkaTopicUri.getTopic(), topicUriPartition.getPartition())
                ),
                activeTopicUrisOrPartitions.put(topicUriPartition)
        );
    }

    @Override
    public Map<TopicUriPartition, MessageId> getMessageIdByTimestamp(Map<TopicUriPartition, Long> timestampByTopicUriPartition) throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("getMessageIdByTimestamp()");

        Map<TopicPartition, TopicUriPartition> topicPartitionToTopicUriPartition = new HashMap<>();
        for (TopicUriPartition topicUriPartition : timestampByTopicUriPartition.keySet()) {
            topicPartitionToTopicUriPartition.put(
                    new TopicPartition(
                            topicUriPartition.getTopicUri().getTopic(),
                            topicUriPartition.getPartition()
                    ),
                    topicUriPartition
            );
        }

        Map<TopicPartition, OffsetAndTimestamp> offsetByTopicPartition =
                executeBackendCallWithRetriesAndReturn(
                        () -> kafkaConsumer.offsetsForTimes(
                                timestampByTopicUriPartition.entrySet().stream().collect(Collectors.toMap(
                                        entry -> (new TopicPartition(
                                                entry.getKey().getTopicUri().getTopic(),
                                                entry.getKey().getPartition()
                                        )),
                                        Map.Entry::getValue
                                ))
                        ),
                        activeTopicUrisOrPartitions.put(timestampByTopicUriPartition.keySet())
                );

        Map<TopicUriPartition, MessageId> messageIdByTopicUriPartition = new HashMap<>();
        offsetByTopicPartition.forEach(((topicPartition, offsetAndTimestamp) ->
                messageIdByTopicUriPartition.put(
                        topicPartitionToTopicUriPartition.get(topicPartition),
                        offsetAndTimestamp == null ? null : new KafkaMessageId(topicPartitionToTopicUriPartition.get(topicPartition), offsetAndTimestamp.offset())
                )
        ));

        return messageIdByTopicUriPartition;
    }

    @Override
    public Map<MetricName, Metric> metrics() throws ConsumerException {
        if (kafkaConsumer == null)
            handleUninitializedKafkaConsumer("metrics()");

        if (metricValueProvider.getMetrics().isEmpty()) {
            // bootstrap metrics and force load
            reportConsumerMetrics();
        }
        return metricValueProvider.getMetrics();
    }

    private Set<TopicPartition> waitForAssignment() throws ConsumerException {
        Set<TopicPartition> assignment = executeBackendCallWithRetriesAndReturn(() -> kafkaConsumer.assignment());
        while (assignment.isEmpty()) {
            executeBackendCallWithRetries(() -> kafkaConsumer.poll(Duration.ofMillis(kafkaPollTimeoutMs)));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                handleException(e, true);
            }
            assignment = executeBackendCallWithRetriesAndReturn(() -> kafkaConsumer.assignment());
        }
        return assignment;
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

    protected void maybeResetBackendClient() throws ConsumerException {
        // reset if SSL enabled && cert is expired
        if (isSslEnabledInAnyActiveSusbcriptionOrAssignment &&
                (System.currentTimeMillis() >= sslCertificateExpiryTimeInMillis)) {
            if (!pscConfigurationInternal.isProactiveSslResetEnabled()) {
                logger.info("Skipping reset of client even though SSL certificate is approaching expiry at {}" +
                        " because proactive reset is disabled", sslCertificateExpiryTimeInMillis);
                return;
            }
            if (KafkaSslUtils.keyStoresExist(properties)) {
                logger.info("Resetting backend Kafka client due to cert expiry at " +
                        sslCertificateExpiryTimeInMillis);
                resetBackendClient();

                // update with new SSL expiry timestamp
                sslCertificateExpiryTimeInMillis = KafkaSslUtils.calculateSslCertExpiryTime(
                        properties, pscConfigurationInternal, getActiveTopicUrisOrPartitions());
            } else {
                logger.warn("Cannot verify existence of key store and trust store. " +
                        "Continue to run with old, expired certificate with expiry time " +
                        sslCertificateExpiryTimeInMillis);
            }
        }
    }

    @VisibleForTesting
    protected boolean isSslEnabledInAnyActiveSusbcriptionOrAssignment() {
        return isSslEnabledInAnyActiveSusbcriptionOrAssignment;
    }

    private boolean detectIfSslEnabledInActiveAssignmentOrSubscription() {
        // go through all active subscriptions/assignments and see if any are using SSL
        for (Object topicUriOrPartition: getActiveTopicUrisOrPartitions()) {
            String protocol = "";
            if (topicUriOrPartition instanceof TopicUri)
                protocol = ((TopicUri) topicUriOrPartition).getProtocol();
            else if (topicUriOrPartition instanceof TopicUriPartition)
                protocol = ((TopicUriPartition) topicUriOrPartition).getTopicUri().getProtocol();

            if (protocol.equals(KafkaTopicUri.SECURE_PROTOCOL))
                return true;
        }
        return false;
    }

    @Override
    protected void resetBackendClient() throws ConsumerException {
        super.resetBackendClient();
        logger.warn("Resetting the backend Kafka consumer (potentially to retry an API if an earlier call failed).");
        executeBackendCallWithRetries(() -> kafkaConsumer.close());
        kafkaConsumer = new KafkaConsumer<>(properties);
        if (!currentAssignment.isEmpty())
            assign(currentAssignment);
        else if (!currentSubscription.isEmpty())
            subscribe(currentSubscription);
    }

    private KafkaMessageId getKafkaMessageId(MessageId messageId) {
        return messageId instanceof KafkaMessageId ?
                (KafkaMessageId) messageId :
                new KafkaMessageId(messageId);
    }

    private void handleException(Exception exception, boolean emitMetrics) throws ConsumerException {
        handleException(exception, currentAssignment.isEmpty() ? currentSubscription : currentAssignment, emitMetrics);
    }

    private void handleUninitializedKafkaConsumer(String callerMethod) throws ConsumerException {
        handleException(
                new BackendConsumerException(
                        String.format("[Kafka] Consumer is not initialized prior to call to %s.", callerMethod),
                        PscUtils.BACKEND_TYPE_KAFKA
                ), true
        );
    }

    protected Set getActiveTopicUrisOrPartitions() {
        return currentAssignment.isEmpty() ? currentSubscription : currentAssignment;
    }

    private void incrementCounterMetricForSubscriptionsOrAssignments(String metricName) {
        for (TopicUriPartition tup : currentAssignment) {
            PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                    tup.getTopicUri(), metricName, pscConfigurationInternal
            );
        }
        for (TopicUri topicUri : currentSubscription) {
            PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                    topicUri, metricName, pscConfigurationInternal
            );
        }
    }
}
