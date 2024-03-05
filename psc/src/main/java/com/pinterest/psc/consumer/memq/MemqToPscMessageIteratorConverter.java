package com.pinterest.psc.consumer.memq;

import com.google.common.collect.Iterators;
import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.CloseableIterator;
import com.pinterest.psc.common.PscMessage;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.event.PscEvent;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.consumer.ToPscMessageIteratorConverter;
import com.pinterest.psc.interceptor.ConsumerInterceptors;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.logging.PscLogger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MemqToPscMessageIteratorConverter<K, V> extends ToPscMessageIteratorConverter<K, V> {

    private static final PscLogger logger = PscLogger.getLogger(MemqToPscMessageIteratorConverter.class);
    private final String topicName;
    private final CloseableIterator<MemqLogMessage<byte[], byte[]>> memqConsumerRecordIterator;
    private Iterator<MemqLogMessage<byte[], byte[]>> filteredIterator;
    private final Map<String, TopicUri> memqTopicToTopicUri;
    private final Map<Integer, MemqOffset> initialSeekOffsets;

    public MemqToPscMessageIteratorConverter(
            String topicName,
            CloseableIterator<MemqLogMessage<byte[], byte[]>> memqConsumerRecordIterator,
            Map<String, TopicUri> memqTopicToTopicUri,
            ConsumerInterceptors<K, V> consumerInterceptors,
            Map<Integer, MemqOffset> initialSeekOffsets
    ) {
        super(consumerInterceptors);
        this.topicName = topicName;
        this.memqConsumerRecordIterator = memqConsumerRecordIterator;
        this.memqTopicToTopicUri = memqTopicToTopicUri;
        this.initialSeekOffsets = initialSeekOffsets;
        this.filteredIterator = memqConsumerRecordIterator;
        if (!this.initialSeekOffsets.isEmpty()) {
            this.filteredIterator = Iterators.filter(this.filteredIterator, (m) -> {
                MemqOffset offset = this.initialSeekOffsets.get(m.getNotificationPartitionId());
                if (
                    offset != null && (
                        offset.getBatchOffset() > m.getNotificationPartitionOffset() ||
                            (offset.getBatchOffset() == m.getNotificationPartitionOffset() && offset.getMessageOffset() > m.getMessageOffsetInBatch())
                    )
                ) {
                    return false;
                }
                if (offset != null) {
                    initialSeekOffsets.remove(m.getNotificationPartitionId());
                }
                return true;
            });
        }
    }

    @Override
    public boolean hasNext() {
        return filteredIterator.hasNext();
    }

    @Override
    public PscConsumerPollMessageIterator<K, V> iteratorFor(TopicUriPartition topicUriPartition) {
        return null;
    }

    @Override
    public Set<TopicUriPartition> getTopicUriPartitions() {
        return null;
    }

    @Override
    protected PscConsumerMessage<byte[], byte[]> getNextBackendMessage() {
        MemqLogMessage<byte[], byte[]> memqConsumerRecord = filteredIterator.next();
        return convertMemqConsumerRecordToPscConsumerMessage(memqConsumerRecord);
    }

    private PscConsumerMessage<byte[], byte[]> convertMemqConsumerRecordToPscConsumerMessage(MemqLogMessage<byte[], byte[]> memqConsumerRecord) {
        byte[] key = memqConsumerRecord.getKey();
        byte[] value = memqConsumerRecord.getValue();

        TopicUri topicUri = memqTopicToTopicUri.get(topicName);

        TopicUriPartition topicUriPartition = new TopicUriPartition(
                topicUri.getTopicUriAsString(),
                memqConsumerRecord.getNotificationPartitionId());
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);

        MemqOffset memqOffset = new MemqOffset(memqConsumerRecord.getNotificationPartitionOffset(), memqConsumerRecord.getMessageOffsetInBatch());

        MemqMessageId messageId = new MemqMessageId(
                topicUriPartition,
                memqOffset.toLong(),
                memqConsumerRecord.getWriteTimestamp(),
                memqConsumerRecord.getKey() == null ? -1 : memqConsumerRecord.getKey().length,
                memqConsumerRecord.getValue() == null ? -1 : memqConsumerRecord.getValue().length,
                memqConsumerRecord.isEndOfBatch()
        );
        PscConsumerMessage<byte[], byte[]> pscConsumerMessage = new PscConsumerMessage<>(
                messageId, key, value, memqConsumerRecord.getWriteTimestamp()
        );

        if (memqConsumerRecord.isEndOfBatch()) {
            pscConsumerMessage.setHeader(PscEvent.EVENT_HEADER, PscMemqConsumer.END_OF_BATCH_EVENT.getBytes());
        }

        Map<String, byte[]> headers = memqConsumerRecord.getHeaders();
        if (headers != null) {
            headers.forEach(pscConsumerMessage::setHeader);
        }

        IntegerSerializer integerSerializer = new IntegerSerializer();
        pscConsumerMessage.setHeader(PscMessage.PSC_MESSAGE_HEADER_KEY_SIZE_BYTES,
                integerSerializer.serialize(messageId.getSerializedKeySizeBytes()));
        pscConsumerMessage.setHeader(PscMessage.PSC_MESSAGE_HEADER_VALUE_SIZE_BYTES,
                integerSerializer.serialize(messageId.getSerializedValueSizeBytes()));

        return pscConsumerMessage;
    }

    @Override
    public void close() throws IOException {
        memqConsumerRecordIterator.close();
    }
}
