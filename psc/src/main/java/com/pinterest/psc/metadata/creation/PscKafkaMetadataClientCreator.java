package com.pinterest.psc.metadata.creation;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metadata.client.kafka.PscKafkaMetadataClient;

/**
 * A class that creates a {@link com.pinterest.psc.metadata.client.PscBackendMetadataClient} for Kafka.
 */
@PscMetadataClientCreatorPlugin(backend = PscUtils.BACKEND_TYPE_KAFKA)
public class PscKafkaMetadataClientCreator extends PscBackendMetadataClientCreator {

    @Override
    public PscKafkaMetadataClient create(Environment env, PscConfigurationInternal pscConfigurationInternal, TopicUri clusterUri) throws ConfigurationException {
        PscKafkaMetadataClient pscKafkaMetadataClient = new PscKafkaMetadataClient();
        pscKafkaMetadataClient.initialize(
                clusterUri,
                env,
                pscConfigurationInternal
        );
        return pscKafkaMetadataClient;
    }
}
