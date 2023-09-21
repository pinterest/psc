package com.pinterest.psc.consumer.memq;

import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.CloseableIterator;
import com.pinterest.psc.common.PscMessage;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.consumer.ToPscMessageIteratorConverter;
import com.pinterest.psc.interceptor.ConsumerInterceptors;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.logging.PscLogger;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MemqToPscMessageIteratorConverter<K, V> extends ToPscMessageIteratorConverter<K, V> {

    private static final PscLogger logger = PscLogger.getLogger(MemqToPscMessageIteratorConverter.class);
    private final String topicName;
    private final CloseableIterator<MemqLogMessage<byte[], byte[]>> memqConsumerRecordIterator;
    private final Map<String, TopicUri> memqTopicToTopicUri;

    public MemqToPscMessageIteratorConverter(
            String topicName,
            CloseableIterator<MemqLogMessage<byte[], byte[]>> memqConsumerRecordIterator,
            Map<String, TopicUri> memqTopicToTopicUri,
            ConsumerInterceptors<K, V> consumerInterceptors
    ) {
        super(consumerInterceptors);
        this.topicName = topicName;
        this.memqConsumerRecordIterator = memqConsumerRecordIterator;
        this.memqTopicToTopicUri = memqTopicToTopicUri;
    }

    @Override
    public boolean hasNext() {
        return memqConsumerRecordIterator.hasNext();
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
        MemqLogMessage<byte[], byte[]> memqConsumerRecord = memqConsumerRecordIterator.next();
        return convertMemqConsumerRecordToPscConsumerMessage(memqConsumerRecord);
    }

    private PscConsumerMessage<byte[], byte[]> convertMemqConsumerRecordToPscConsumerMessage(MemqLogMessage<byte[], byte[]> memqConsumerRecord) {
        byte[] key = memqConsumerRecord.getKey();
        byte[] value = memqConsumerRecord.getValue();

        TopicUriPartition topicUriPartition = new TopicUriPartition(
                memqTopicToTopicUri.get(topicName).getTopicUriAsString(),
                memqConsumerRecord.getNotificationPartitionId());
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, memqTopicToTopicUri.get(topicName));

        MemqMessageId messageId = new MemqMessageId(
                topicUriPartition,
                memqConsumerRecord.getNotificationPartitionOffset(),
                memqConsumerRecord.getWriteTimestamp(),
                memqConsumerRecord.getKey() == null ? -1 : memqConsumerRecord.getKey().length,
                memqConsumerRecord.getValue() == null ? -1 : memqConsumerRecord.getValue().length
        );
        PscConsumerMessage<byte[], byte[]> pscConsumerMessage = new PscConsumerMessage<>(
                messageId, key, value, memqConsumerRecord.getWriteTimestamp()
        );

        Map<String, byte[]> headers = memqConsumerRecord.getHeaders();
        if (headers != null) {
            headers.forEach((key1, value1) -> pscConsumerMessage.setHeader(key1, value1));
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
