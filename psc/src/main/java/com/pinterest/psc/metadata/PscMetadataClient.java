package com.pinterest.psc.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.environment.EnvironmentProvider;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metadata.creation.PscBackendMetadataClientCreator;
import com.pinterest.psc.metadata.creation.PscMetadataClientCreatorManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PscMetadataClient implements AutoCloseable {

    private PscMetadataClientCreatorManager creatorManager;
    private Environment environment;
    private final PscConfigurationInternal pscConfigurationInternal;
    private Map<String, PscBackendMetadataClient> pscBackendMetadataClientByTopicUriPrefix = new ConcurrentHashMap<>();

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

    public List<TopicUri> getTopicUris(TopicUri topicUri) {
        PscBackendMetadataClient backendMetadataClient = getBackendMetadataClient(topicUri);
        return null;
    }

    @VisibleForTesting
    protected PscBackendMetadataClient getBackendMetadataClient(TopicUri topicUri) {
        String topicUriPrefix = topicUri.getTopicUriPrefix();
        pscBackendMetadataClientByTopicUriPrefix.computeIfAbsent(topicUriPrefix, k -> {
           PscBackendMetadataClientCreator backendMetadataClientCreator = creatorManager.getBackendCreators().get(topicUri.getBackend());
            try {
                return backendMetadataClientCreator.create(environment, pscConfigurationInternal, topicUri);
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
        });
        return pscBackendMetadataClientByTopicUriPrefix.get(topicUriPrefix);
    }

    @Override
    public void close() throws Exception {

    }
}
