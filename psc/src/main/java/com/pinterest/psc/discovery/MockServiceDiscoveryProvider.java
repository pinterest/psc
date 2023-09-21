package com.pinterest.psc.discovery;

import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.logging.PscLogger;

@ServiceDiscoveryPlugin(priority = 101)
public class MockServiceDiscoveryProvider implements ServiceDiscoveryProvider {
    private static final PscLogger logger = PscLogger.getLogger(MockServiceDiscoveryProvider.class);
    private String connectionUrl;
    private String securityProtocol;

    public MockServiceDiscoveryProvider() {
    }

    @Override
    public void configure(PscConfiguration pscConfiguration) {
        connectionUrl = pscConfiguration.getString("connection.url");
        securityProtocol = pscConfiguration.getString("security.protocol");
    }

    @Override
    public ServiceDiscoveryConfig getConfig(Environment env, TopicUri topicUri) {
        if (connectionUrl == null || securityProtocol == null)
            return null;

        return new ServiceDiscoveryConfig()
                .setServiceDiscoveryProvider(this)
                .setConnect(connectionUrl)
                .setSecurityProtocol(securityProtocol);
    }
}
