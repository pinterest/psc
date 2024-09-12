package com.pinterest.psc.metadata.client;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metadata.TopicRnMetadata;
import com.pinterest.psc.metadata.creation.PscBackendMetadataClientCreator;
import com.pinterest.psc.metadata.creation.PscMetadataClientCreatorManager;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A client for metadata queries and operations, similar to the KafkaAdminClient but this is backend-agnostic.
 *
 * This class is responsible for creating and managing the lifecycle of the backend-specific metadata clients,
 * such as the {@link com.pinterest.psc.metadata.client.kafka.PscKafkaMetadataClient}, all of which should extend
 * {@link com.pinterest.psc.metadata.client.PscBackendMetadataClient}.
 *
 * Each backend-specific metadata client is responsible for handling the actual metadata queries and operations.
 *
 * The creation of {@link PscBackendMetadataClient} relies on the supplied {@link TopicUri} at the time of
 * method call to determine which backend-specific metadata client to create. The creation logic resides in
 * the {@link PscMetadataClientCreatorManager} and the {@link PscBackendMetadataClientCreator} implementations.
 * This model follows the same pattern as how backend clients are created in {@link com.pinterest.psc.consumer.PscConsumer}
 * and {@link com.pinterest.psc.producer.PscProducer}.
 *
 * As such, each API method in this class will delegate to the appropriate backend-specific metadata client, and must
 * accept a {@link TopicUri} to determine which backend-specific metadata client to use.
 *
 * The {@link TopicUri} supplied to each method does not need to be a full URI including the topic name. Instead,
 * it should only include the URI prefix (up to the cluster name), since connecting to the cluster is the only
 * information needed to determine which backend-specific metadata client to use. Supplying a full URI including the
 * topic name will not cause any issues, but it is unnecessary.
 */
public class PscMetadataClient implements AutoCloseable {
    private PscMetadataClientCreatorManager creatorManager;
    private Environment environment;
    private final PscConfigurationInternal pscConfigurationInternal;
    private final Map<String, PscBackendMetadataClient> pscBackendMetadataClientByTopicUriPrefix = new ConcurrentHashMap<>();

    public PscMetadataClient(PscConfiguration pscConfiguration) throws ConfigurationException {
        this.pscConfigurationInternal = new PscConfigurationInternal(
                pscConfiguration,
                PscConfigurationInternal.PSC_CLIENT_TYPE_METADATA
        );
        initialize();
    }

    private void initialize() {
        creatorManager = new PscMetadataClientCreatorManager();
        environment = pscConfigurationInternal.getEnvironment();
    }

    /**
     * List all the {@link TopicRn}'s in the cluster.
     *
     * @param clusterUri
     * @param duration
     * @return the list of {@link TopicRn}'s in the cluster
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public List<TopicRn> listTopicRns(TopicUri clusterUri, Duration duration) throws ExecutionException, InterruptedException, TimeoutException {
        PscBackendMetadataClient backendMetadataClient = getBackendMetadataClient(clusterUri);
        return backendMetadataClient.listTopicRns(duration);
    }

    /**
     * Describe the metadata for the given {@link TopicRn}'s in the cluster.
     *
     * @param clusterUri
     * @param topicRns
     * @param duration
     * @return a map of {@link TopicRn} to {@link TopicRnMetadata}
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public Map<TopicRn, TopicRnMetadata> describeTopicRns(TopicUri clusterUri, Set<TopicRn> topicRns, Duration duration) throws ExecutionException, InterruptedException, TimeoutException {
        PscBackendMetadataClient backendMetadataClient = getBackendMetadataClient(clusterUri);
        return backendMetadataClient.describeTopicRns(topicRns, duration);
    }

    /**
     * List the offsets for the given {@link TopicUriPartition}'s in the cluster.
     *
     * For each {@link TopicUriPartition}, the user can specify an {@link MetadataClientOption} to specify whether to
     * get the earliest or latest offset.
     *
     * For example, if the user specifies {@link MetadataClientOption#OFFSET_SPEC_EARLIEST}, the client will return the earliest
     * offset for the partition.
     *
     * @param clusterUri
     * @param topicRnsAndOptions
     * @param duration
     * @return a map of {@link TopicUriPartition} to {@link MessageId}. The {@link MessageId} will contain the offset but
     * not necessarily the timestamp (timestamp might be null or unset)
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public Map<TopicUriPartition, Long> listOffsets(TopicUri clusterUri, Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicRnsAndOptions, Duration duration) throws ExecutionException, InterruptedException, TimeoutException {
        PscBackendMetadataClient backendMetadataClient = getBackendMetadataClient(clusterUri);
        return backendMetadataClient.listOffsets(topicRnsAndOptions, duration);
    }

    /**
     * List the offsets for the given {@link TopicUriPartition}'s in the cluster for the given consumer group ID.
     *
     * @param clusterUri
     * @param consumerGroup
     * @param topicUriPartitions
     * @param duration
     * @return a map of {@link TopicUriPartition} to {@link MessageId}. The {@link MessageId} will contain the offset but
     * not necessarily the timestamp (timestamp might be null or unset)
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public Map<TopicUriPartition, Long> listOffsetsForConsumerGroup(TopicUri clusterUri, String consumerGroup, Collection<TopicUriPartition> topicUriPartitions, Duration duration) throws ExecutionException, InterruptedException, TimeoutException {
        PscBackendMetadataClient backendMetadataClient = getBackendMetadataClient(clusterUri);
        return backendMetadataClient.listOffsetsForConsumerGroup(consumerGroup, topicUriPartitions, duration);
    }

    @VisibleForTesting
    protected PscBackendMetadataClient getBackendMetadataClient(TopicUri clusterUri) {
        String topicUriPrefix = clusterUri.getTopicUriPrefix();
        pscBackendMetadataClientByTopicUriPrefix.computeIfAbsent(topicUriPrefix, k -> {
           PscBackendMetadataClientCreator backendMetadataClientCreator = creatorManager.getBackendCreators().get(clusterUri.getBackend());
            try {
                return backendMetadataClientCreator.create(environment, pscConfigurationInternal, clusterUri);
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
        });
        return pscBackendMetadataClientByTopicUriPrefix.get(topicUriPrefix);
    }

    @Override
    public void close() throws IOException {
        for (PscBackendMetadataClient client : pscBackendMetadataClientByTopicUriPrefix.values()) {
            client.close();
        }
        pscBackendMetadataClientByTopicUriPrefix.clear();
    }

    /**
     * An enum to specify the options for the metadata client API's
     */
    public enum MetadataClientOption {
        OFFSET_SPEC_EARLIEST,
        OFFSET_SPEC_LATEST
    }
}
