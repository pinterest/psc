package com.pinterest.psc.common;

import com.pinterest.psc.discovery.ServiceDiscoveryProvider;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Representation of the configurations that define how PSC should attempt to connect to the backend service
 */
public class ServiceDiscoveryConfig {
    private ServiceDiscoveryProvider serviceDiscoveryProvider;
    private String connect;
    private String securityProtocol;
    private Map<String, String> secureConfigs;

    /**
     * @return the {@link ServiceDiscoveryProvider} associated with this config
     */
    public ServiceDiscoveryProvider getServiceDiscoveryProvider() {
        return serviceDiscoveryProvider;
    }

    /**
     * @param serviceDiscoveryProvider
     * @return this config, to allow chaining
     */
    public ServiceDiscoveryConfig setServiceDiscoveryProvider(ServiceDiscoveryProvider serviceDiscoveryProvider) {
        this.serviceDiscoveryProvider = serviceDiscoveryProvider;
        return this;
    }

    /**
     * @return the connection endpoint(s) associated with this config
     */
    public String getConnect() {
        return connect;
    }

    /**
     * Sets the connection endpoint(s) associated with this config. This should be in the form of {@code hostname:port}
     * @param connect the connection endpoint(s) associated with this config
     * @return this config, to allow chaining
     */
    public ServiceDiscoveryConfig setConnect(String connect) {
        this.connect = connect;
        return this;
    }

    /**
     * @return the security protocol associated with this config
     */
    public String getSecurityProtocol() {
        return securityProtocol;
    }

    /**
     * Sets the backend-specific security protocol used to communicate with the backend service.
     * @param securityProtocol
     * @return this config, to allow chaining
     */
    public ServiceDiscoveryConfig setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
        return this;
    }

    public Map<String, String> getSecureConfigs() {
        return secureConfigs;
    }

    public ServiceDiscoveryConfig setSecureConfigs(Map<String, String> secureConfigs) {
        this.secureConfigs = secureConfigs;
        return this;
    }

    @Override
    public String toString() {
        return String.format("[connect: %s, securityProtocol: %s, secureConfigs: %s]",
                connect,
                securityProtocol,
                PscUtils.toString(secureConfigs)
        );
    }
}
