package com.pinterest.psc.metadata.client;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.discovery.ServiceDiscoveryManager;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metadata.TopicRnMetadata;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * An abstract class that defines the interface for metadata queries and operations. Specific implementations
 * of this class should be created for each backend, such as Kafka, MemQ, etc.
 */
public abstract class PscBackendMetadataClient implements AutoCloseable {

    private static final PscLogger logger = PscLogger.getLogger(PscBackendMetadataClient.class);

    protected TopicUri topicUri;
    protected PscConfigurationInternal pscConfigurationInternal;
    protected ServiceDiscoveryConfig discoveryConfig;

    public void initialize(TopicUri topicUri, Environment env, PscConfigurationInternal pscConfigurationInternal) throws ConfigurationException {
        logger.info("Initializing metadata client for topicUri: " + topicUri);
        this.topicUri = topicUri;
        this.pscConfigurationInternal = pscConfigurationInternal;
        this.discoveryConfig =
                ServiceDiscoveryManager.getServiceDiscoveryConfig(env, pscConfigurationInternal.getDiscoveryConfiguration(), topicUri);
    }

    public abstract List<TopicRn> listTopicRns(Duration duration)
            throws ExecutionException, InterruptedException, TimeoutException;

    public abstract Map<TopicRn, TopicRnMetadata> describeTopicRns(
            Collection<TopicRn> topicRns,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract Map<TopicUriPartition, Long> listOffsets(
            Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicRnsAndOptions,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract Map<TopicUriPartition, Long> listOffsetsForTimestamps(
            Map<TopicUriPartition, Long> topicUriPartitionsAndTimes,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract Map<TopicUriPartition, Long> listOffsetsForConsumerGroup(
            String consumerGroupId,
            Collection<TopicUriPartition> topicUriPartitions,
            Duration duration
    ) throws ExecutionException, InterruptedException, TimeoutException;

    public abstract void close() throws IOException;

    @VisibleForTesting
    protected TopicUri getTopicUri() {
        return topicUri;
    }
}
