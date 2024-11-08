package com.pinterest.psc.metadata;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;

import java.util.List;

/**
 * Metadata for a {@link TopicUri}, including the list of its partitions
 */
public class TopicUriMetadata {

    private final TopicUri topicUri;
    private final List<TopicUriPartition> topicUriPartitions;

    public TopicUriMetadata(TopicUri topicUri, List<TopicUriPartition> topicUriPartitions) {
        this.topicUri = topicUri;
        this.topicUriPartitions = topicUriPartitions;
    }

    public TopicUri getTopicUri() {
        return topicUri;
    }

    public List<TopicUriPartition> getTopicUriPartitions() {
        return topicUriPartitions;
    }
}
