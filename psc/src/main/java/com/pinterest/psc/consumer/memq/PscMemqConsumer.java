package com.pinterest.psc.consumer.memq;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.pinterest.memq.client.commons.ConsumerConfigs;
import com.pinterest.memq.client.commons.serde.ByteArrayDeserializer;
import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.memq.client.consumer.NoTopicsSubscribedException;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.CloseableIterator;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.event.PscEvent;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConsumerToMemqConsumerConfigConverter;
import com.pinterest.psc.consumer.ConsumerRebalanceListener;
import com.pinterest.psc.consumer.OffsetCommitCallback;
import com.pinterest.psc.consumer.PscBackendConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.logging.Log;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PscMemqConsumer<K, V> extends PscBackendConsumer<K, V> {

    public static final String END_OF_BATCH_EVENT = "end_of_batch";

    private static final PscLogger logger = PscLogger.getLogger(PscMemqConsumer.class);
    @VisibleForTesting
    protected MemqConsumer<byte[], byte[]> memqConsumer;
    private final Set<TopicUri> currentSubscription = new HashSet<>();
    private final Set<TopicUriPartition> currentAssignment = new HashSet<>();
    private String[] topicsUri;
    private Properties properties;
    private TopicUri topicUri;

    private final Map<Integer, MemqOffset> initialSeekOffsets = new ConcurrentHashMap<>();

    public PscMemqConsumer() {
    }

    public void setTopicsUri(String[] topicsUri) {
        this.topicsUri = topicsUri;
    }

    public String[] getTopicsUri() {
        return topicsUri;
    }

    @Override
    public void initializeBackend(ServiceDiscoveryConfig discoveryConfig,
                                  TopicUri topicUri) throws ConsumerException {
        this.topicUri = new MemqTopicUri(topicUri);
        properties = new PscConsumerToMemqConsumerConfigConverter().convert(pscConfigurationInternal, topicUri);
        properties.setProperty(ConsumerConfigs.BOOTSTRAP_SERVERS, discoveryConfig.getConnect());
        properties.setProperty(ConsumerConfigs.KEY_DESERIALIZER_CLASS_KEY,
                ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfigs.KEY_DESERIALIZER_CLASS_CONFIGS_KEY, new Properties());
        properties.setProperty(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_KEY,
                ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfigs.VALUE_DESERIALIZER_CLASS_CONFIGS_KEY, new Properties());
        properties.setProperty(ConsumerConfigs.CLIENT_ID,
                pscConfigurationInternal.getPscConsumerClientId() + "-" + new Random().nextLong());
        properties.setProperty(ConsumerConfigs.DIRECT_CONSUMER, "false");

        try {
            memqConsumer = new MemqConsumer<>(properties);
        } catch (InstantiationError e) {
            throw new ConsumerException(e);
        } catch (Exception e) {
            throw new ConsumerException("Could not instantiate a Memq consumer instance.", e);
        }
        initializeMetricRegistry(memqConsumer);
    }

    private void initializeMetricRegistry(MemqConsumer<?, ?> memqConsumer) {

        memqConsumer.getMetricRegistry().addListener(new MetricRegistryListener.Base() {
            @Override
            public void onGaugeAdded(String name, Gauge<?> gauge) {
                MemqMetricsHandler.addMetricNameConversion(name);
            }

            @Override
            public void onCounterAdded(String name, Counter counter) {
                MemqMetricsHandler.addMetricNameConversion(name);
            }

            @Override
            public void onHistogramAdded(String name, Histogram histogram) {
                MemqMetricsHandler.addMetricNameConversion(name);
            }

            @Override
            public void onMeterAdded(String name, Meter meter) {
                MemqMetricsHandler.addMetricNameConversion(name);
            }

            @Override
            public void onTimerAdded(String name, Timer timer) {
                MemqMetricsHandler.addMetricNameConversion(name);
            }
        });
    }

    @Override
    protected void reportConsumerMetrics() {
        MemqMetricsHandler.handleMemqConsumerMetrics(
                memqConsumer.getMetricRegistry(),
                memqConsumer.getTopicName(),
                backendTopicToTopicUri,
                pscConfigurationInternal
        );
    }

    @Override
    public void subscribe(Set<TopicUri> topicUris) throws ConsumerException {
        if (memqConsumer == null) {
            throw new ConsumerException(
                    "[Memq] Consumer is not initialized prior to call to subscribe().");
        }

        if (topicUris.isEmpty()) {
            unsubscribe();
            return;
        }

        Set<String> topics = new HashSet<>();
        for (TopicUri topicUri : topicUris) {
            if (!(topicUri instanceof MemqTopicUri)) {
                throw new ConsumerException("[Memq] " + topicUri + " is not a valid Memq URI");
            }
            String topic = topicUri.getTopic();
            topics.add(topic);
            backendTopicToTopicUri.put(topic, topicUri);
        }

        // update the subscription list with the new topic uris
        currentSubscription.retainAll(topicUris);
        currentSubscription.addAll(topicUris);
        try {
            memqConsumer.subscribe(topics);
        } catch (Exception exception) {
            throw new ConsumerException("[Memq] Exception occurred subscribing to topics.", exception);
        }
    }
    public void subscribe(Set<TopicUri> value, ConsumerRebalanceListener consumerRebalanceListener)
        throws ConsumerException {
        if (consumerRebalanceListener != null)
            throw new ConsumerException("[Memq] ConsumerRebalanceListener not supported");
        subscribe(value);
    }

    @Override
    public void unsubscribe() throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException(
                    "[Memq] Consumer is not initialized prior to call to unsubscribe().");

        memqConsumer.unsubscribe();
        currentSubscription.clear();
    }

    @Override
    public Set<TopicUri> subscription() throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException(
                    "[Memq] Consumer is not initialized prior to call to subscription().");

        return currentSubscription;
    }

    @Override
    public void assign(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (memqConsumer == null) {
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to assign().");
        }

        if (!currentSubscription.isEmpty()) {
            throw new ConsumerException(
                    "[Memq] Consumer assign() is not supported when consumer is already subscribed to topics.");
        }

        String memqTopic;
        Set<TopicUri> topicUriSet = topicUriPartitions.stream().map(TopicUriPartition::getTopicUri).collect(Collectors.toSet());
        if (topicUriSet.size() > 1) {
            throw new ConsumerException("[Memq] Consumer can only support subscribing/assigning to one topic");
        } else {
            memqTopic = topicUriSet.iterator().next().getTopic();
        }

        if (topicUriPartitions.isEmpty()) {
            unsubscribe();
            return;
        }

        for (TopicUriPartition topicUriPartition : topicUriPartitions) {
            backendTopicToTopicUri.put(topicUriPartition.getTopicUri().getTopic(),
                topicUriPartition.getTopicUri());
        }

        // update the assignment list with the new topic uris
        currentAssignment.retainAll(topicUriPartitions);
        currentAssignment.addAll(topicUriPartitions);
        try {
            memqConsumer.subscribe(memqTopic);
            memqConsumer.assign(topicUriPartitions.stream().map(TopicUriPartition::getPartition)
                    .collect(Collectors.toList()));
        } catch (Exception exception) {
            throw new ConsumerException("[Memq] Consumer assign() failed.", exception);
        }
    }

    @Override
    public void unassign() throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException(
                    "[Memq] Consumer is not initialized prior to call to unassign().");

        memqConsumer.unsubscribe();
        currentAssignment.clear();
    }

    @Override
    public Set<TopicUriPartition> assignment() throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException(
                    "[Memq] Consumer is not initialized prior to call to assignment().");
        Set<Integer> assignment = memqConsumer.assignment();
        return assignment.stream().map(a -> {
            TopicUriPartition tup = new TopicUriPartition(topicUri.getTopicUriAsString(), a);
            BaseTopicUri.finalizeTopicUriPartition(tup, topicUri);
            return tup;
        }).collect(Collectors.toSet());
    }

    @Override
    public PscConsumerPollMessageIterator<K, V> poll(Duration pollTimeout) throws ConsumerException,
            WakeupException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to poll().");

        long startTs = System.currentTimeMillis();
        CloseableIterator<MemqLogMessage<byte[], byte[]>> memqLogMessageIterator;
        try {
            MutableInt count = new MutableInt();
            memqLogMessageIterator = new MemqIteratorAdapter<>(memqConsumer.poll(pollTimeout, count));
        } catch (NoTopicsSubscribedException e) {
            throw new ConsumerException("[Memq] Consumer is not subscribed to any topic.", e);
        } catch (IOException e) {
            throw new ConsumerException("[Memq] IO exception occurred when polling for messages.", e);
        } catch (org.apache.kafka.common.errors.WakeupException wakeupException) {
            throw new WakeupException(wakeupException);
        }
        long stopTs = System.currentTimeMillis();

        for (TopicUri memqTopicUri : this.currentSubscription) {
            PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(memqTopicUri,
                    PscMetrics.PSC_CONSUMER_POLL_TIME_MS_METRIC, stopTs - startTs, pscConfigurationInternal);
        }

        return new MemqToPscMessageIteratorConverter<>(memqConsumer.getTopicName(),
            memqLogMessageIterator, backendTopicToTopicUri, getConsumerInterceptors(),
            initialSeekOffsets);
    }

    private void handleMemqConsumerMetrics(MetricRegistry metricRegistry) {

    }

    public void seek(MessageId messageId) throws ConsumerException {
        seek(Collections.singleton(messageId));
    }

    @Override
    public void seek(Set<MessageId> messageIds) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to seek().");
        Set<MemqMessageId> memqMessageIds = messageIds.stream().map(this::getMemqMessageId).collect(Collectors.toSet());
        seekToOffset(
                memqMessageIds.stream()
                        .collect(Collectors.toMap(
                                MessageId::getTopicUriPartition,
                                this::getNextOffsetFromMessageId
                        ))
        );
    }

    private long getNextOffsetFromMessageId(MemqMessageId memqMessageId) {
        MemqOffset memqOffset = MemqOffset.convertPscOffsetToMemqOffset(memqMessageId.getOffset());
        if (memqMessageId.isEndOfBatch()) {
            return memqOffset.getBatchOffset() + 1;
        } else {
            memqOffset.setMessageOffset(memqOffset.getMessageOffset() + 1);
            return memqOffset.toLong();
        }
    }

    @Override
    public void seekToTimestamp(TopicUri topicUri, long timestamp) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to seekToTimestamp().");

        if (!currentSubscription.contains(topicUri)) {
            throw new ConsumerException("[Memq] Consumer is not subscribed to topic URI " + topicUri + " passed to seekToTimestamp().");
        }

        Set<Integer> assignedPartitions;
        try {
            assignedPartitions = memqConsumer.waitForAssignment();
        } catch (NoTopicsSubscribedException e) {
            throw new ConsumerException(e);
        }

        Map<Integer, Long> timestamps = new HashMap<>();

        for (Integer partition : assignedPartitions) {
            timestamps.put(partition, timestamp);
        }

        Map<Integer, Long> offsets = executeBackendCallWithRetriesAndReturn(
            () -> memqConsumer.offsetsOfTimestamps(timestamps)
        );

        executeBackendCallWithRetries(() -> memqConsumer.seek(offsets));
    }

    @Override
    public void seekToTimestamp(Map<TopicUriPartition, Long> seekPositions) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to seekToTimestamp().");

        if (!currentSubscription.contains(topicUri)) {
            throw new ConsumerException("[Memq] Consumer is not subscribed to topic URI " + topicUri + " passed to seekToTimestamp().");
        }

        Map<Integer, Long> timestamps = new HashMap<>();

        for (Map.Entry<TopicUriPartition, Long> entry : seekPositions.entrySet()) {
            if (!isCurrentTopicPartition(entry.getKey())) {
                throw new ConsumerException("[Memq] Consumer is not subscribed to TopicUriPartition" + entry.getKey());
            }
            timestamps.put(entry.getKey().getPartition(), entry.getValue());
        }

        try {
            memqConsumer.waitForAssignment();
        } catch (NoTopicsSubscribedException e) {
            throw new ConsumerException(e);
        }


        Map<Integer, Long> offsets = executeBackendCallWithRetriesAndReturn(
            () -> memqConsumer.offsetsOfTimestamps(timestamps)
        );

        executeBackendCallWithRetries(() -> memqConsumer.seek(offsets));
    }

    @Override
    public void seekToOffset(Map<TopicUriPartition, Long> seekPositions) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to seekToOffset().");

        for (TopicUriPartition topicUriPartition : seekPositions.keySet()) {
            if (!(topicUriPartition.getTopicUri() instanceof MemqTopicUri)) {
                throw new ConsumerException(
                        "[Memq] Message id with illegal topic URI " + topicUriPartition.getTopicUri() + " was passed in to seekToOffset().");
            }

            backendTopicToTopicUri.put(topicUriPartition.getTopicUri().getTopic(),
                    topicUriPartition.getTopicUri());

            if (!isCurrentTopicPartition(topicUriPartition)) {
                throw new ConsumerException(String.format(
                        "[Memq] Consumer is not subscribed to the topicUri '%s' or "
                                + "assigned the TopicUriPartition '%s' passed to seek().",
                        topicUriPartition.getTopicUri(), topicUriPartition));
            }
        }

        try {
            memqConsumer.waitForAssignment();
        } catch (NoTopicsSubscribedException e) {
            throw new ConsumerException(e);
        }

        seekToMemqOffset(
            seekPositions.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> MemqOffset.convertPscOffsetToMemqOffset(v.getValue())))
        );
    }

    private void seekToMemqOffset(Map<TopicUriPartition, MemqOffset> seekPositions) throws ConsumerException {
        Map<Integer, Long> batchOffsetMap = new HashMap<>();
        for (Map.Entry<TopicUriPartition, MemqOffset> entry : seekPositions.entrySet()) {
            MemqOffset memqOffset = entry.getValue();
            if (memqOffset.getMessageOffset() != 0) {
                initialSeekOffsets.put(entry.getKey().getPartition(), memqOffset);
            }
            batchOffsetMap.put(entry.getKey().getPartition(), memqOffset.getBatchOffset());
        }

        memqConsumer.seek(batchOffsetMap);
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        throw new ConsumerException("[Memq] Consumer async commit is not supported.");
    }

    @Override
    public void commitAsync(Set<MessageId> messageIds,
                            OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to commitAsync().");

        Map<Integer, Long> maxOffsets = new HashMap<>();
        for (MessageId messageId : messageIds) {
            TopicUri topicUri = messageId.getTopicUriPartition().getTopicUri();
            if (!(topicUri instanceof MemqTopicUri)) {
                throw new ConsumerException(
                    "[Memq] Message id with illegal topic URI " + topicUri + " was passed in to commitAsync().");
            }
            if (!this.topicUri.equals(topicUri)) {
                throw new ConsumerException("[Memq] Consumer is not associated with topic URI " + topicUri
                    + " passed in to commitAsync().");
            }

            MemqMessageId memqMessageId = getMemqMessageId(messageId);

            int partition = memqMessageId.getTopicUriPartition().getPartition();
            long offset = memqMessageId.getOffset();
            MemqOffset memqOffset = MemqOffset.convertPscOffsetToMemqOffset(offset);
            long kafkaOffset = memqMessageId.isEndOfBatch() ? memqOffset.getBatchOffset() + 1: memqOffset.getBatchOffset();
            maxOffsets.compute(partition,
                (key, val) -> (val == null) ? kafkaOffset : (kafkaOffset > val) ? kafkaOffset : val);
        }

        if (offsetCommitCallback != null) {
            executeBackendCallWithRetries(() -> {
                memqConsumer.commitOffsetAsync(maxOffsets, ((offsets, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                    Map<TopicUriPartition, MessageId> pscOffsets = new HashMap<>(offsets.size());
                    offsets.forEach(((partition, offset) -> {
                        TopicUriPartition topicUriPartition = new TopicUriPartition(
                            topicUri.getTopicUriAsString(), partition);
                        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
                        pscOffsets.put(topicUriPartition, new MemqMessageId(topicUriPartition, offset, true));
                    }));
                    offsetCommitCallback.onCompletion(pscOffsets, exception);
                }));
            });
        } else {
            memqConsumer.commitOffsetAsync(maxOffsets, (offsets, exception) -> {});
        }
    }

    @Override
    public Set<MessageId> commitSync() throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to commitSync().");
        if (memqConsumer.getNotificationSource() == null) {
            throw new ConsumerException(
                "[Memq] Consumer notificationSource is not initialized prior to call to commitSync().");
        }
        memqConsumer.commitOffset();
        //TODO: extract latest committed offsets from MemQ consumer when the API is available. This is currently unavailable
        return new HashSet<>();
    }

    @Override
    public void commitSync(Set<MessageId> messageIds) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to commitSync().");

        Map<Integer, Long> maxOffsets = new HashMap<>();
        for (MessageId messageId : messageIds) {
            TopicUri topicUri = messageId.getTopicUriPartition().getTopicUri();
            if (!(topicUri instanceof MemqTopicUri)) {
                throw new ConsumerException(
                        "[Memq] Message id with illegal topic URI " + topicUri + " was passed in to commitSync().");
            }
            if (!this.topicUri.equals(topicUri)) {
                throw new ConsumerException("[Memq] Consumer is not associated with topic URI " + topicUri
                        + " passed in to commitSync().");
            }

            MemqMessageId memqMessageId = getMemqMessageId(messageId);

            int partition = memqMessageId.getTopicUriPartition().getPartition();
            long offset = memqMessageId.getOffset();
            MemqOffset memqOffset = MemqOffset.convertPscOffsetToMemqOffset(offset);
            long kafkaOffset = memqMessageId.isEndOfBatch() ? memqOffset.getBatchOffset() + 1: memqOffset.getBatchOffset();
            maxOffsets.compute(partition,
                (key, val) -> (val == null) ? kafkaOffset : (kafkaOffset > val) ? kafkaOffset : val);
        }
        memqConsumer.commitOffset(maxOffsets);
    }

    @Override
    public void seekToBeginning(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to seekToBeginning().");
        Set<Integer> partitions = new HashSet<>();
        for (TopicUriPartition tup : topicUriPartitions) {
            if (!isCurrentTopicPartition(tup)) {
                throw new ConsumerException("[Memq] Cannot seek on non-associated topic partition " + tup);
            }
            partitions.add(tup.getPartition());
        }
        memqConsumer.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to seekToEnd().");
        Set<Integer> partitions = new HashSet<>();
        for (TopicUriPartition tup : topicUriPartitions) {
            if (!isCurrentTopicPartition(tup)) {
                throw new ConsumerException("[Memq] Cannot seek on non-associated topic partition " + tup);
            }
            partitions.add(tup.getPartition());
        }
        memqConsumer.seekToEnd(partitions);
    }

    @Override
    public void pause(Collection<TopicUriPartition> topicUriPartitions) {
        throw new UnsupportedOperationException("[Memq] Consumer does not support pause/resume.");
    }

    @Override
    public void resume(Collection<TopicUriPartition> topicUriPartitions) {
        throw new UnsupportedOperationException("[Memq] Consumer does not support pause/resume.");
    }

    @Override
    // Uses a short-living consumer to support metadata calls without subscribing
    public Set<TopicUriPartition> getPartitions(TopicUri topicUri) throws ConsumerException {
        try (MemqConsumer<byte[], byte[]> consumer = getMetadataConsumer(topicUri)) {
            MemqTopicUri memqTopicUri = (MemqTopicUri) topicUri;

            return consumer.getPartition().stream().map(partition -> {
                TopicUriPartition topicUriPartition = new TopicUriPartition(
                    memqTopicUri.getTopicUriAsString(), partition);
                BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, memqTopicUri);
                return topicUriPartition;
            }).collect(Collectors.toSet());
        } catch (IOException ioe) {
            throw new ConsumerException("[Memq] Failed to close metadata consumer for " + topicUri, ioe);
        }
    }

    @Override
    public Set<String> getTopicNames() throws ConsumerException {
        throw new ConsumerException("[Memq] Topic name retrieval not supported");
    }

    @Override
    public void wakeup() {
        memqConsumer.wakeup();
    }

    @Override
    public void close() throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to close().");
        currentSubscription.clear();
        try {
            memqConsumer.close();
        } catch (IOException e) {
            throw new ConsumerException(e);
        }
    }

    @Override
    public void close(Duration timeout) throws ConsumerException {
        close();
        super.close(timeout);
    }

    @Override
    public MessageId committed(TopicUriPartition topicUriPartition) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to committed().");
        if (isCurrentTopicPartition(topicUriPartition)) {
            long committedOffset = memqConsumer.committed(topicUriPartition.getPartition());
            return committedOffset == -1L ? null : new MemqMessageId(topicUriPartition, new MemqOffset(committedOffset, 0).toLong(), true);
        }
        throw new ConsumerException("[Memq] Failed to retrieve committed offsets for unassociated TopicUriPartition: " + topicUriPartition);
    }

    @Override
    public Collection<MessageId> committed(Collection<TopicUriPartition> topicUriPartitions) throws ConsumerException, WakeupException {
        throw new UnsupportedOperationException("[Memq] Consumer does not support batch committed offsets retrieval.");
    }

    @Override
    public Map<TopicUriPartition, Long> startOffsets(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to startOffsets().");

        Map<Integer, TopicUriPartition> partitionToTopicUriPartition = topicUriPartitions.stream()
                .collect(Collectors.toMap(TopicUriPartition::getPartition, Function.identity()));

        Map<Integer, Long> startOffsets = memqConsumer
                .getEarliestOffsets(partitionToTopicUriPartition.keySet());
        return startOffsets.entrySet().stream().collect(Collectors
                .toMap(entry -> partitionToTopicUriPartition.get(entry.getKey()), Map.Entry::getValue));
    }

    @Override
    public Map<TopicUriPartition, Long> endOffsets(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to getLatestOffsets().");

        Map<Integer, TopicUriPartition> partitionToTopicUriPartition = topicUriPartitions.stream()
                .collect(Collectors.toMap(TopicUriPartition::getPartition, Function.identity()));

        Map<Integer, Long> endOffsets = memqConsumer
                .getLatestOffsets(partitionToTopicUriPartition.keySet());
        return endOffsets.entrySet().stream().collect(Collectors
                .toMap(entry -> partitionToTopicUriPartition.get(entry.getKey()), Map.Entry::getValue));
    }

    @Override
    public long position(TopicUriPartition topicUriPartition) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to position().");
        if (isCurrentTopicPartition(topicUriPartition)) {
            return new MemqOffset(memqConsumer.position(topicUriPartition.getPartition()), 0).toLong();
        }
        throw new ConsumerException("[Memq] Consumer is not associated with " + topicUriPartition);
    }

    @Override
    public Map<TopicUriPartition, MessageId> getMessageIdByTimestamp(Map<TopicUriPartition, Long> timestampByTopicUriPartition) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to getMessageIdByTimestamp().");

        TopicUri topicUri = null;
        Map<Integer, Long> partitionToTimestampMap = new HashMap<>();
        Map<Integer, TopicUriPartition> partitionToTopicUriPartitionMap = new HashMap<>();
        for (Map.Entry<TopicUriPartition, Long> entry : timestampByTopicUriPartition.entrySet()) {
            if (topicUri == null) topicUri = entry.getKey().getTopicUri();
            else if (topicUri != entry.getKey().getTopicUri()) {
                throw new ConsumerException("[Memq] Consumer cannot handle multiple topics in getMessageIdByTimestamp()");
            }
            partitionToTopicUriPartitionMap.put(entry.getKey().getPartition(), entry.getKey());
            partitionToTimestampMap.put(entry.getKey().getPartition(), entry.getValue());
        }

        Map<Integer, Long> offsetByTopicPartition =
            executeBackendCallWithRetriesAndReturn(
                () -> memqConsumer.offsetsOfTimestamps(partitionToTimestampMap)
            );

        Map<TopicUriPartition, MessageId> messageIdByTopicUriPartition = new HashMap<>();
        offsetByTopicPartition.forEach(((partition, offsetAndTimestamp) ->
            messageIdByTopicUriPartition.put(
                partitionToTopicUriPartitionMap.get(partition),
                offsetAndTimestamp == null ? null : new MemqMessageId(partitionToTopicUriPartitionMap.get(partition), new MemqOffset(offsetAndTimestamp, 0).toLong(), false)
            )
        ));

        return messageIdByTopicUriPartition;
    }

    @VisibleForTesting
    protected void setMemqConsumer(MemqConsumer<byte[], byte[]> memqConsumer) {
        this.memqConsumer = memqConsumer;
    }

    @Override
    public PscConfiguration getConfiguration() {
        PscConfiguration pscConfiguration = new PscConfiguration();
        properties.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value.toString()));
        return pscConfiguration;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() throws ConsumerException {
        return Collections.emptyMap();
    }

    /**
     * Allows event-based logic to be executed by the backend producer/consumer
     * This method is evolving
     */
    @Override
    public void onEvent(PscEvent event) {
        if (event.getType().equals(END_OF_BATCH_EVENT)) {
            try {
                memqConsumer.commitOffsetAsync(Collections.emptyMap(), (o, e) -> {});
            } catch (Exception e) {
                // do nothing if this happens since it is best effort
            }
        }
    }

    public boolean isNotificationSourceInitialized() {
        return memqConsumer.getNotificationSource() != null;
    }

    private MemqMessageId getMemqMessageId(MessageId messageId) {
        return messageId instanceof MemqMessageId ?
                (MemqMessageId) messageId :
                new MemqMessageId(messageId);
    }

    private boolean isCurrentTopicPartition(TopicUriPartition topicUriPartition) {
        return this.currentSubscription.contains(topicUriPartition.getTopicUri()) || this.currentAssignment.contains(topicUriPartition);
    }

    private MemqConsumer<byte[], byte[]> getMetadataConsumer(TopicUri topicUri) throws ConsumerException {
        try {
            Properties tmpProps = new Properties(properties);
            tmpProps.setProperty(ConsumerConfigs.CLIENT_ID, tmpProps.getProperty(ConsumerConfigs.CLIENT_ID) + "_metadata");
            tmpProps.setProperty(ConsumerConfigs.GROUP_ID, topicUri.getTopic() + "_metadata_cg_" + ThreadLocalRandom.current().nextInt());
            MemqConsumer<byte[], byte[]> consumer = new MemqConsumer<>(tmpProps);
            consumer.subscribe(topicUri.getTopic());
            return consumer;
        } catch (InstantiationError e) {
            throw new ConsumerException(e);
        } catch (Exception e) {
            throw new ConsumerException("Could not instantiate a Memq consumer instance.", e);
        }
    }
}
