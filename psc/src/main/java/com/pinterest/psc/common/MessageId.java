package com.pinterest.psc.common;

import java.io.Serializable;

public class MessageId implements Serializable {
    private static final long serialVersionUID = 8439747255283115793L;
    protected TopicUriPartition topicUriPartition;
    protected long offset;
    protected long timestamp = -1;
    protected int serializedKeySizeBytes = -2; // unset
    protected int serializedValueSizeBytes = -2;  // unset

    public MessageId(TopicUriPartition topicUriPartition, long offset, long timestamp, int serializedKeySizeBytes, int serializedValueSizeBytes) {
        this(topicUriPartition, offset, timestamp);
        this.serializedKeySizeBytes = serializedKeySizeBytes;
        this.serializedValueSizeBytes = serializedValueSizeBytes;
    }

    public MessageId(TopicUriPartition topicUriPartition, long offset, int serializedKeySizeBytes, int serializedValueSizeBytes) {
        this(topicUriPartition, offset);
        this.serializedKeySizeBytes = serializedKeySizeBytes;
        this.serializedValueSizeBytes = serializedValueSizeBytes;
    }

    public MessageId(TopicUriPartition topicUriPartition, long offset, long timestamp) {
        this(topicUriPartition, offset);
        this.timestamp = timestamp;
    }

    public MessageId(TopicUriPartition topicUriPartition, long offset) {
        this.topicUriPartition = topicUriPartition;
        this.offset = offset;
    }

    public MessageId(String topicUriString, int partition, long offset) {
        this(new TopicUriPartition(topicUriString, partition), offset);
    }

    public TopicUriPartition getTopicUriPartition() {
        return topicUriPartition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getSerializedKeySizeBytes() {
        return serializedKeySizeBytes;
    }

    public int getSerializedValueSizeBytes() {
        return serializedValueSizeBytes;
    }

    @Override
    public boolean equals(Object that) {
        if (that == null)
            return false;
        if (that instanceof MessageId)
            return this.equals((MessageId) that);
        return false;
    }

    public boolean equals(MessageId that) {
        if (that == null)
            return false;
        if (this == that)
            return true;

        if (!PscCommon.equals(this.topicUriPartition, that.topicUriPartition))
            return false;
        return this.offset == that.offset && this.timestamp == that.timestamp;
    }

    @Override
    public String toString() {
        return timestamp == -1 ?
                String.format("[topicUri-partition: %s, offset: %d]", topicUriPartition, offset) :
                String.format("[topicUri-partition: %s, offset: %d, timestamp: %d]", topicUriPartition, offset, timestamp);
    }
}
