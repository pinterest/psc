package com.pinterest.psc.metadata.creation;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.discovery.ServiceDiscoveryManager;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metadata.kafka.PscKafkaMetadataClient;

@PscMetadataClientCreatorPlugin(backend = PscUtils.BACKEND_TYPE_KAFKA)
public class PscKafkaMetadataClientCreator extends PscBackendMetadataClientCreator {

    @Override
    public PscKafkaMetadataClient create(Environment env, PscConfigurationInternal pscConfigurationInternal, TopicUri topicUri) throws ConfigurationException {
        PscKafkaMetadataClient pscKafkaMetadataClient = new PscKafkaMetadataClient();
        pscKafkaMetadataClient.initialize(
                topicUri,
                ServiceDiscoveryManager.getServiceDiscoveryConfig(env, pscConfigurationInternal.getDiscoveryConfiguration(), topicUri)
        );
        return pscKafkaMetadataClient;
    }
}
