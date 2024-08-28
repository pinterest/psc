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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    public List<TopicRn> listTopicRns(TopicUri clusterUri, long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
        PscBackendMetadataClient backendMetadataClient = getBackendMetadataClient(clusterUri);
        return backendMetadataClient.listTopicRns(timeout, timeUnit);
    }

    public Map<TopicRn, TopicRnMetadata> describeTopicRns(TopicUri clusterUri, Set<TopicRn> topicRns, long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
        PscBackendMetadataClient backendMetadataClient = getBackendMetadataClient(clusterUri);
        return backendMetadataClient.describeTopicRns(topicRns, timeout, timeUnit);
    }

    public Map<TopicUriPartition, MessageId> listOffsets(TopicUri clusterUri, Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicRnsAndOptions, long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
        PscBackendMetadataClient backendMetadataClient = getBackendMetadataClient(clusterUri);
        return backendMetadataClient.listOffsets(topicRnsAndOptions, timeout, timeUnit);
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
    public void close() throws Exception {
        for (PscBackendMetadataClient client : pscBackendMetadataClientByTopicUriPrefix.values()) {
            client.close();
        }
        pscBackendMetadataClientByTopicUriPrefix.clear();
    }

    public enum MetadataClientOption {
        OFFSET_SPEC_EARLIEST,
        OFFSET_SPEC_LATEST
    }
}
