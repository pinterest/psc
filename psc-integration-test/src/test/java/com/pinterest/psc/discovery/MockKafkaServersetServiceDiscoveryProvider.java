package com.pinterest.psc.discovery;

import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ServiceDiscoveryException;
import com.pinterest.psc.logging.PscLogger;

@ServiceDiscoveryPlugin(priority = 100)
public class MockKafkaServersetServiceDiscoveryProvider extends MockServersetServiceDiscoveryProvider {
    private static final PscLogger logger = PscLogger.getLogger(MockKafkaServersetServiceDiscoveryProvider.class);

    @Override
    public String getServersetFilename(Environment env, TopicUri topicUri) throws ServiceDiscoveryException {
        if (topicUri instanceof KafkaTopicUri) {
            KafkaTopicUri kafkaUri = (KafkaTopicUri) topicUri;
            return getServersetFilename(env, kafkaUri.getTopicRn().getEnvironment(), kafkaUri.getBackend(), kafkaUri.getRegion(), kafkaUri.getCluster(),
                    kafkaUri.getProtocol());
        } else {
            throw new ServiceDiscoveryException(topicUri + " is not an KafkaTopicUri instance");
        }
    }

    @Override
    protected ServiceDiscoveryConfig configureServiceDiscoveryConfig(Environment env, TopicUri uri) {
        ServiceDiscoveryConfig discoveryConfig = super.configureServiceDiscoveryConfig(env, uri);
        if (discoveryConfig != null && uri.getProtocol() != null && uri.getProtocol().equals(KafkaTopicUri.SECURE_PROTOCOL)) {
            discoveryConfig.setSecurityProtocol("ssl");
        }
        return discoveryConfig;
    }

    @Override
    protected String getServersetFilename(Environment environment, String deploymentStage, String backend, String region, String cluster,
                                          String transportProtocol) {
        region = region.toLowerCase();
        cluster = cluster.toLowerCase();
        transportProtocol = transportProtocol.toLowerCase();

        return String.format("%sdiscovery.%s%s.serverset",
                region.equals(environment.getRegion()) ? "" : region + ".",
                cluster,
                (transportProtocol.equals(KafkaTopicUri.SECURE_PROTOCOL)) ? "_tls" : ""
        );
    }

}
