package com.pinterest.psc.metadata;

import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUriPartition;

import java.util.List;

/**
 * Metadata for a {@link TopicRn}, including the list of its partitions
 */
public class TopicRnMetadata {

    private final TopicRn topicRn;
    private final List<TopicUriPartition> topicUriPartitions;

    public TopicRnMetadata(TopicRn topicRn, List<TopicUriPartition> topicUriPartitions) {
        this.topicRn = topicRn;
        this.topicUriPartitions = topicUriPartitions;
    }

    public TopicRn getTopicRn() {
        return topicRn;
    }

    public List<TopicUriPartition> getTopicUriPartitions() {
        return topicUriPartitions;
    }
}
