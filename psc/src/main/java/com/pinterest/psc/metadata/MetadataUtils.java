package com.pinterest.psc.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

/**
 * Utility class for common metadata logic
 */
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

    public static TopicUri createTopicUri(String topic, TopicRn clusterRn, String protocol) {
        try {
            return BaseTopicUri.validate(protocol + ":" + TopicUri.SEPARATOR + clusterRn.getTopicRnPrefixString() + topic);
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException("Failed to create topic URI for topic " + topic, e);
        }
    }
}
