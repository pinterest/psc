package com.pinterest.psc.metadata.creation;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metadata.client.kafka.PscKafkaMetadataClient;

/**
 * A class that creates a {@link com.pinterest.psc.metadata.client.PscBackendMetadataClient} for Kafka.
 */
@PscMetadataClientCreatorPlugin(backend = PscUtils.BACKEND_TYPE_KAFKA)
public class PscKafkaMetadataClientCreator extends PscBackendMetadataClientCreator {

    private static final PscLogger logger = PscLogger.getLogger(PscKafkaMetadataClientCreator.class);

    @Override
    public PscKafkaMetadataClient create(Environment env, PscConfigurationInternal pscConfigurationInternal, TopicUri clusterUri) throws ConfigurationException {
        logger.info("Creating Kafka metadata client for clusterUri: " + clusterUri);
        PscKafkaMetadataClient pscKafkaMetadataClient = new PscKafkaMetadataClient();
        pscKafkaMetadataClient.initialize(
                clusterUri,
                env,
                pscConfigurationInternal
        );
        return pscKafkaMetadataClient;
    }

    @Override
    public TopicUri validateBackendTopicUri(TopicUri topicUri) throws TopicUriSyntaxException {
        String topicUriStr = topicUri.getTopicUriAsString();
        if (topicUri.getProtocol().equals(KafkaTopicUri.SECURE_PROTOCOL)) {
            // always use PLAINTEXT for metadata requests
            topicUriStr = topicUriStr.replace(KafkaTopicUri.SECURE_PROTOCOL + ":" + TopicUri.SEPARATOR, KafkaTopicUri.PLAINTEXT_PROTOCOL + ":" + TopicUri.SEPARATOR);
        }
        return KafkaTopicUri.validate(BaseTopicUri.validate(topicUriStr));
    }
}
