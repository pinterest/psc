package com.pinterest.psc.common;

import com.pinterest.psc.metrics.NullMetricsReporter;

public class TestUtils {
    public static final String DEFAULT_METRICS_REPORTER = NullMetricsReporter.class.getName();

    public static TopicUriPartition getFinalizedTopicUriPartition(TopicUri topicUri, int partition) {
        return new TopicUriPartition(topicUri, partition);
    }
}
