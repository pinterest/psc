package com.pinterest.psc.discovery;

import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.logging.PscLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ServiceDiscoveryPlugin(priority = 102)
public class Mock2ServiceDiscoveryProvider implements ServiceDiscoveryProvider {
    private static final PscLogger logger = PscLogger.getLogger(Mock2ServiceDiscoveryProvider.class);
    private Map<String, List<ClusterConnectionInfo>> topicUriPrefixToClusterConnectionInfo;

    public Mock2ServiceDiscoveryProvider() {
    }

    @Override
    public void configure(PscConfiguration pscConfiguration) {
        String[] topicUriPrefixes = PscConfigurationInternal.getMultiValueConfiguration(pscConfiguration,"topic.uri.prefixes");
        String[] connectionUrls = PscConfigurationInternal.getMultiValueConfiguration(pscConfiguration, "connection.urls");
        String[] securityProtocols = PscConfigurationInternal.getMultiValueConfiguration(pscConfiguration, "security.protocols");
        if (topicUriPrefixes.length != connectionUrls.length || connectionUrls.length != securityProtocols.length)
            return;
        topicUriPrefixToClusterConnectionInfo = new HashMap<>();
        for (int i = 0; i < topicUriPrefixes.length; ++i) {
            topicUriPrefixToClusterConnectionInfo.computeIfAbsent(
                    topicUriPrefixes[i],
                    k -> new ArrayList<>()
            ).add(new ClusterConnectionInfo(connectionUrls[i], securityProtocols[i]));
        }
    }

    @Override
    public ServiceDiscoveryConfig getConfig(Environment env, TopicUri topicUri) {
        if (topicUriPrefixToClusterConnectionInfo == null)
            return null;
        String topicUriPrefix = topicUri.getTopicUriPrefix();
        if (!topicUriPrefixToClusterConnectionInfo.containsKey(topicUriPrefix)) {
            return null;
        }
        List<ClusterConnectionInfo> clusterConnectionInfos = topicUriPrefixToClusterConnectionInfo.get(topicUriPrefix);
        // multiple security protocols not suppported at the moment, since it returns a single ServiceDiscoveryConfig
        return new ServiceDiscoveryConfig()
                .setServiceDiscoveryProvider(this)
                .setConnect(clusterConnectionInfos.stream().map(info -> info.connectionUrl).collect(Collectors.joining(",")))
                .setSecurityProtocol(String.join(",", clusterConnectionInfos.stream().map(info -> info.securityProtocol).collect(Collectors.toSet())));
    }

    static class ClusterConnectionInfo {
        String connectionUrl;
        String securityProtocol;

        public ClusterConnectionInfo(String connectionUrl, String securityProtocol) {
            this.connectionUrl = connectionUrl;
            this.securityProtocol = securityProtocol;
        }
    }
}
