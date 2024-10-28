package com.pinterest.flink.connector.psc;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

import java.util.Properties;

public class PscFlinkConfiguration {

    /**
     * Configuration key for the cluster URI. This is required for the following client types that use the {@link com.pinterest.flink.connector.psc.source.PscSource}
     * and {@link com.pinterest.flink.connector.psc.sink.PscSink} classes:
     *
     * <li>Transactional producer: The cluster URI specifies the cluster to which the producer will connect. Transactional
     *      *  producers require a cluster URI to be specified because transaction lifecycles are managed by the backend
     *      *  PubSub cluster, and it is generally not possible to have transactions span multiple clusters.</li>
     * <li>Consumer: Unlike regular PscConsumers, consumers in {@link com.pinterest.flink.connector.psc.source.PscSource} require
     *      * clusterUri to connect to the cluster for metadata queries during their lifecycles.</li>
     */
    public static final String CLUSTER_URI_CONFIG = "psc.cluster.uri";

    public static TopicUri validateAndGetBaseClusterUri(Properties properties) throws TopicUriSyntaxException {
        if (!properties.containsKey(CLUSTER_URI_CONFIG)) {
            throw new IllegalArgumentException("Cluster URI is required for transactional producer");
        }
        return BaseTopicUri.validate(properties.getProperty(CLUSTER_URI_CONFIG));
    }
}
