package com.pinterest.psc.consumer.memq;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;

public class MemqMessageId extends MessageId {

    private final boolean endOfBatch;

    private static final long serialVersionUID = 7724281495471377020L;

    public MemqMessageId(TopicUriPartition topicUriPartition, long offset, long timestamp, int serializedKeySizeBytes, int serializedValueSizeBytes, boolean endOfBatch) {
        super(topicUriPartition, offset, timestamp, serializedKeySizeBytes, serializedValueSizeBytes);
        this.endOfBatch = endOfBatch;
    }

    public MemqMessageId(TopicUriPartition topicUriPartition, long offset, long timestamp, boolean endOfBatch) {
        super(topicUriPartition, offset, timestamp);
        this.endOfBatch = endOfBatch;
    }

    public MemqMessageId(TopicUriPartition topicUriPartition, long offset, boolean endOfBatch) {
        super(topicUriPartition, offset);
        this.endOfBatch = endOfBatch;
    }

    public MemqMessageId(MessageId messageId) {
        super(messageId.getTopicUriPartition(), messageId.getOffset(), messageId.getTimestamp());
        this.endOfBatch = false;
    }

    public boolean isEndOfBatch() {
        return endOfBatch;
    }

    public String toString() {
        return timestamp == -1 ?
            String.format("[topicUri-partition: %s, offset: %d, isEndOfBatch: %b]", topicUriPartition, offset, endOfBatch) :
            String.format("[topicUri-partition: %s, offset: %d, timestamp: %d, isEndOfBatch: %b]", topicUriPartition, offset, timestamp, endOfBatch);
    }
}
