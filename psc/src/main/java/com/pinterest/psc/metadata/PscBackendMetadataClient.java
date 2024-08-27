package com.pinterest.psc.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class PscBackendMetadataClient implements AutoCloseable {

    private TopicUri topicUri;
    private ServiceDiscoveryConfig serviceDiscoveryConfig;

    public void initialize(TopicUri topicUri, ServiceDiscoveryConfig discoveryConfig) {
        this.topicUri = topicUri;
        this.serviceDiscoveryConfig = discoveryConfig;
    }

    public abstract List<TopicRn> getTopicRns(long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract void close() throws Exception;

    @VisibleForTesting
    protected TopicRn getTopicRn(String topicName) {
        String topicRnPrefix = topicUri.getTopicRn().getTopicRnPrefixString();
        return new TopicRn(
                topicRnPrefix,
                topicRnPrefix,
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

    @VisibleForTesting
    protected TopicUri getTopicUri() {
        return topicUri;
    }

    @VisibleForTesting
    protected ServiceDiscoveryConfig getServiceDiscoveryConfig() {
        return serviceDiscoveryConfig;
    }
}
