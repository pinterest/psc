package com.pinterest.psc.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;

public class MetadataUtils {

    @VisibleForTesting
    public static TopicRn createTopicRn(TopicUri topicUri, String topicName) {
        return new TopicRn(
                topicUri.getTopicRn().getTopicRnPrefixString() + topicName,
                topicUri.getTopicRn().getTopicRnPrefixString(),
                topicUri.getTopicRn().getStandard(),
                topicUri.getTopicRn().getService(),
                topicUri.getTopicRn().getEnvironment(),
                topicUri.getTopicRn().getCloud(),
                topicUri.getTopicRn().getRegion(),
                topicUri.getTopicRn().getClassifier(),
                topicUri.getTopicRn().getCluster(),
                topicName
        );
    }
}
