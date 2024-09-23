package com.pinterest.flink.connector.psc;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import java.util.Properties;

public class PscFlinkConfiguration {
    public static final String CLUSTER_URI_CONFIG = "psc.producer.cluster.uri";

    public static TopicUri validateAndGetBaseClusterUri(Properties properties) throws TopicUriSyntaxException {
        if (!properties.containsKey(CLUSTER_URI_CONFIG)) {
            throw new IllegalArgumentException("Cluster URI is required for transactional producer");
        }
        return BaseTopicUri.validate(properties.getProperty(CLUSTER_URI_CONFIG));
    }
}
