package com.pinterest.psc.metadata.client;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.discovery.ServiceDiscoveryManager;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metadata.TopicRnMetadata;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class PscBackendMetadataClient implements AutoCloseable {

    protected TopicUri topicUri;
    protected PscConfigurationInternal pscConfigurationInternal;
    protected ServiceDiscoveryConfig discoveryConfig;

    public void initialize(TopicUri topicUri, Environment env, PscConfigurationInternal pscConfigurationInternal) throws ConfigurationException {
        this.topicUri = topicUri;
        this.pscConfigurationInternal = pscConfigurationInternal;
        this.discoveryConfig =
                ServiceDiscoveryManager.getServiceDiscoveryConfig(env, pscConfigurationInternal.getDiscoveryConfiguration(), topicUri);
    }

    public abstract List<TopicRn> listTopicRns(
            long timeout,
            TimeUnit timeUnit
    ) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract Map<TopicRn, TopicRnMetadata> describeTopicRns(
            Collection<TopicRn> topicRns,
            long timeout,
            TimeUnit timeUnit
    ) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract Map<TopicUriPartition, MessageId> listOffsets(
            Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicRnsAndOptions,
            long timeout,
            TimeUnit timeUnit
    ) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract Map<TopicUriPartition, MessageId> listOffsetsForConsumerGroup(
            String consumerGroupId,
            Collection<TopicUriPartition> topicUriPartitions,
            long timeout,
            TimeUnit timeUnit
    ) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract void close() throws Exception;

    @VisibleForTesting
    protected TopicUri getTopicUri() {
        return topicUri;
    }

    @VisibleForTesting
    protected ServiceDiscoveryConfig getDiscoveryConfig() {
        return discoveryConfig;
    }
}
