package com.pinterest.psc.common;

import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import java.io.Serializable;

/**
 * A topic URI and partition pair to identify the most granular pub/sub concept plus the access protocol.
 */
public class TopicUriPartition implements Comparable<TopicUriPartition>, Serializable {
    private static final long serialVersionUID = -7784054113828809322L;
    private final String topicUriStr;
    private final int partition;
    private TopicUri backendTopicUri;

    /**
     * Builds a TopicUriPartition instance with the default partition value (-1). This is meant to be used in
     * scenarios where partition does not apply or is not supported.
     *
     * @param topicUriStr the topic URI in string format
     */
    public TopicUriPartition(String topicUriStr) {
        this.topicUriStr = topicUriStr;
        this.partition = PscUtils.NO_PARTITION;
        try {
            this.backendTopicUri = TopicUri.validate(topicUriStr);
        } catch (TopicUriSyntaxException e) {
            // propagate the null value
        }
    }

    /**
     * Builds a TopicUriPartition instance based on the given topic URI and partition.
     *
     * @param topicUriStr the topic URI in string format
     * @param partition   the partition
     */
    public TopicUriPartition(String topicUriStr, int partition) {
        this.topicUriStr = topicUriStr;
        this.partition = partition;
        try {
            this.backendTopicUri = TopicUri.validate(topicUriStr);
        } catch (TopicUriSyntaxException e) {
            // propagate the null value
        }
    }

    public TopicUriPartition(TopicUri topicUri, int partition) {
        this.backendTopicUri = topicUri;
        this.topicUriStr = topicUri.getTopicUriAsString();
        this.partition = partition;
    }

    protected void setTopicUri(TopicUri backendTopicUri) {
        this.backendTopicUri = backendTopicUri;
    }

    /**
     * Returns the topic URI associated with this TopicUriPartition
     *
     * @return topic URI in string format
     */
    public String getTopicUriAsString() {
        return topicUriStr;
    }

    /**
     * Returns the partition associated with this TopicUriPartition
     *
     * @return partition
     */
    public int getPartition() {
        return partition;
    }

    public TopicUri getTopicUri() {
        return backendTopicUri;
    }

    @Override
    public int compareTo(TopicUriPartition o) {
        if (!PscCommon.equals(backendTopicUri, o.backendTopicUri))
            return -1;

        if (this.topicUriStr.equals(o.getTopicUriAsString()) && this.partition == o.partition)
            return 0;

        int topicUriComparison = this.topicUriStr.compareTo(o.getTopicUriAsString());
        if (topicUriComparison != 0)
            return topicUriComparison;

        return this.partition - o.partition;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof TopicUriPartition)) return false;

        TopicUriPartition otherTopicUriPartition = (TopicUriPartition) other;

        return PscCommon.equals(topicUriStr, otherTopicUriPartition.topicUriStr) &&
                PscCommon.equals(backendTopicUri, otherTopicUriPartition.backendTopicUri) &&
                partition == otherTopicUriPartition.partition;
    }

    @Override
    public int hashCode() {
        int result = topicUriStr.hashCode();
        result = 31 * result + (backendTopicUri == null ? 0 : backendTopicUri.hashCode());
        result = 31 * result + partition;
        return result;
    }

    @Override
    public String toString() {
        return String.format("[TopicUri: %s, Partition: %s, Finalized?: %s]", topicUriStr, partition, !(backendTopicUri == null));
    }
}