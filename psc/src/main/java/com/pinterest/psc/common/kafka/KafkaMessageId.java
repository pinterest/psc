package com.pinterest.psc.common.kafka;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;

public class KafkaMessageId extends MessageId {
    private static final long serialVersionUID = 2382070822439582943L;

    public KafkaMessageId(TopicUriPartition topicUriPartition, long offset, long timestamp, int serializedKeySizeBytes, int serializedValueSizeBytes) {
        super(topicUriPartition, offset, timestamp, serializedKeySizeBytes, serializedValueSizeBytes);
    }

    public KafkaMessageId(TopicUriPartition topicUriPartition, long offset, long timestamp) {
        super(topicUriPartition, offset, timestamp);
    }

    public KafkaMessageId(TopicUriPartition topicUriPartition, long offset) {
        super(topicUriPartition, offset);
    }

    public KafkaMessageId(MessageId messageId) {
        super(messageId.getTopicUriPartition(), messageId.getOffset(), messageId.getTimestamp());
    }
}
