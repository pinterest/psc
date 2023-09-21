package com.pinterest.psc.consumer.memq;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;

public class MemqMessageId extends MessageId {

    private static final long serialVersionUID = 7724281495471377020L;

    public MemqMessageId(TopicUriPartition topicUriPartition, long offset, long timestamp, int serializedKeySizeBytes, int serializedValueSizeBytes) {
        super(topicUriPartition, offset, timestamp, serializedKeySizeBytes, serializedValueSizeBytes);
    }

    public MemqMessageId(TopicUriPartition topicUriPartition, long offset, long timestamp) {
        super(topicUriPartition, offset, timestamp);
    }

    public MemqMessageId(TopicUriPartition topicUriPartition, long offset) {
        super(topicUriPartition, offset);
    }

    public MemqMessageId(MessageId messageId) {
        super(messageId.getTopicUriPartition(), messageId.getOffset(), messageId.getTimestamp());
    }
}
