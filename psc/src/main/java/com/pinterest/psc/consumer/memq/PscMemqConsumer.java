package com.pinterest.psc.consumer.memq;

import com.codahale.metrics.MetricRegistry;
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

    private static final PscLogger logger = PscLogger.getLogger(PscMemqConsumer.class);
    @VisibleForTesting
    protected MemqConsumer<byte[], byte[]> memqConsumer;
    private final Set<TopicUri> currentSubscription = new HashSet<>();
    private final Set<TopicUriPartition> currentAssignment = new HashSet<>();
    private String[] topicsUri;
    private Properties properties;
    private TopicUri topicUri;

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
        this.topicUri = topicUri;
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

        if (topicUriPartitions.isEmpty()) {
            unsubscribe();
            return;
        }

        backendTopicToTopicUri.clear();

        for (TopicUriPartition topicUriPartition : topicUriPartitions)
            backendTopicToTopicUri.put(topicUriPartition.getTopicUri().getTopic(),
                    topicUriPartition.getTopicUri());

        // update the assignment list with the new topic uris
        currentAssignment.retainAll(topicUriPartitions);
        currentAssignment.addAll(topicUriPartitions);
        try {
            memqConsumer.subscribe(topicUriPartitions.stream().map(TopicUriPartition::getTopicUri)
                    .map(TopicUri::getTopic).collect(Collectors.toSet()));
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
        return assignment.stream().map(a -> new TopicUriPartition(topicUri.getTopicUriAsString(), a))
                .collect(Collectors.toSet());
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
                memqLogMessageIterator, backendTopicToTopicUri, getConsumerInterceptors());
    }

    private void handleMemqConsumerMetrics(MetricRegistry metricRegistry) {

    }

    public void seek(MessageId messageId) throws ConsumerException {
        seek(Collections.singleton(messageId));
    }

    @Override
    public void seek(Set<MessageId> messageIds) throws ConsumerException {
        Set<MemqMessageId> memqMessageIds = messageIds.stream().map(this::getMemqMessageId).collect(Collectors.toSet());
        seekToOffset(
                memqMessageIds.stream()
                        .collect(Collectors.toMap(
                                MessageId::getTopicUriPartition,
                                messageId -> 1 + messageId.getOffset()
                        ))
        );
    }

    @Override
    public void seekToTimestamp(TopicUri topicUri, long timestamp) throws ConsumerException {
        throw new ConsumerException("[Memq] Consumer seek by timestamp is not supported.");
    }

    @Override
    public void seekToTimestamp(Map<TopicUriPartition, Long> seekPositions) throws ConsumerException {
        throw new ConsumerException("[Memq] Consumer seek by timestamp is not supported.");
    }

    @Override
    public void seekToOffset(Map<TopicUriPartition, Long> seekPositions) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to seek().");

        for (TopicUriPartition topicUriPartition : seekPositions.keySet()) {
            if (!(topicUriPartition.getTopicUri() instanceof MemqTopicUri)) {
                throw new ConsumerException(
                        "[Memq] Message id with illegal topic URI " + topicUriPartition.getTopicUri() + " was passed in to seekToOffset().");
            }

            backendTopicToTopicUri.put(topicUriPartition.getTopicUri().getTopic(),
                    topicUriPartition.getTopicUri());

            if (!this.currentSubscription.contains(topicUriPartition.getTopicUri())
                    && !this.currentAssignment.contains(topicUriPartition)) {
                throw new ConsumerException(String.format(
                        "[Memq] Consumer is not subscribed to the topicUri '%s' or"
                                + "assigned the TopicUriPartition '%s' passed to seek().",
                        topicUriPartition.getTopicUri(), topicUriPartition));
            }
        }

        try {
            memqConsumer.waitForAssignment();
        } catch (NoTopicsSubscribedException e) {
            throw new ConsumerException(e);
        }

        memqConsumer.seek(
                seekPositions.entrySet().stream()
                        .collect(Collectors.toMap(
                            entry -> entry.getKey().getPartition(),
                            Map.Entry::getValue
                        )
                )
        );
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        throw new ConsumerException("[Memq] Consumer async commit is not supported.");
    }

    @Override
    public void commitAsync(Set<MessageId> messageIds,
                            OffsetCommitCallback offsetCommitCallback) throws ConsumerException {
        throw new ConsumerException("[Memq] Consumer async commit is not supported.");
    }

    @Override
    public Set<MessageId> commitSync() throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to commitSync().");

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
            if (!subscription().contains(topicUri)) {
                throw new ConsumerException("[Memq] Consumer is not subscribed to topic URI " + topicUri
                        + " passed in to commitSync().");
            }

            MemqMessageId memqMessageId = getMemqMessageId(messageId);

            int partition = memqMessageId.getTopicUriPartition().getPartition();
            long offset = memqMessageId.getOffset();
            maxOffsets.compute(partition,
                    (key, val) -> (val == null) ? offset : (offset > val) ? offset : val);
        }
        memqConsumer.commitOffset(maxOffsets);
    }

    @Override
    public void seekToBeginning(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        throw new ConsumerException("[Memq] Consumer seek to beginning is not supported.");
    }

    @Override
    public void seekToEnd(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        throw new ConsumerException("[Memq] Consumer seek to beginning is not supported.");
    }

    @Override
    public Set<TopicUriPartition> getPartitions(TopicUri topicUri) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException(
                    "[Memq] Consumer is not initialized prior to call to getPartitions().");

        MemqTopicUri memqTopicUri = (MemqTopicUri) topicUri;

        return memqConsumer.getPartition().stream().map(partition -> {
            TopicUriPartition topicUriPartition = new TopicUriPartition(
                    memqTopicUri.getTopicUriAsString(), partition);
            BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, memqTopicUri);
            return topicUriPartition;
        }).collect(Collectors.toSet());
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
        throw new ConsumerException("[Memq] Consumer committed is not supported.");
    }

    @Override
    public Map<TopicUriPartition, Long> startOffsets(Set<TopicUriPartition> topicUriPartitions) throws ConsumerException {
        if (memqConsumer == null)
            throw new ConsumerException("[Memq] Consumer is not initialized prior to call to getEarliestOffsets().");

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
        throw new ConsumerException("[Memq] Consumer position is not supported.");
    }

    @Override
    public Map<TopicUriPartition, MessageId> getMessageIdByTimestamp(Map<TopicUriPartition, Long> timestampByTopicUriPartition) throws ConsumerException {
        throw new ConsumerException("[Memq] Consumer getMessageIdByTimestamp is not supported.");
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
        throw new ConsumerException("[Memq] Consumer metrics is not supported.");
    }

    private MemqMessageId getMemqMessageId(MessageId messageId) {
        return messageId instanceof MemqMessageId ?
                (MemqMessageId) messageId :
                new MemqMessageId(messageId);
    }
}
