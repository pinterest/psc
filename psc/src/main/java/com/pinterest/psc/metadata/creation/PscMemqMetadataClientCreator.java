package com.pinterest.psc.metadata.creation;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.memq.MemqTopicUri;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metadata.client.memq.PscMemqMetadataClient;

/**
 * A class that creates a {@link com.pinterest.psc.metadata.client.PscBackendMetadataClient} for Memq.
 */
@PscMetadataClientCreatorPlugin(backend = PscUtils.BACKEND_TYPE_MEMQ, priority = 1)
public class PscMemqMetadataClientCreator extends PscBackendMetadataClientCreator {

    private static final PscLogger logger = PscLogger.getLogger(PscMemqMetadataClientCreator.class);

    @Override
    public PscMemqMetadataClient create(Environment env, PscConfigurationInternal pscConfigurationInternal, TopicUri clusterUri) throws ConfigurationException {
        logger.info("Creating Memq metadata client for clusterUri: " + clusterUri);
        PscMemqMetadataClient pscMemqMetadataClient = new PscMemqMetadataClient();
        pscMemqMetadataClient.initialize(
                clusterUri,
                env,
                pscConfigurationInternal
        );
        return pscMemqMetadataClient;
    }

    @Override
    public TopicUri validateBackendTopicUri(TopicUri topicUri) throws TopicUriSyntaxException {
        return MemqTopicUri.validate(topicUri);
    }
}
