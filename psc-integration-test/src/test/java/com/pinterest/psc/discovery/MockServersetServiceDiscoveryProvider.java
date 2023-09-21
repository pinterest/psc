package com.pinterest.psc.discovery;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ServiceDiscoveryException;
import com.pinterest.psc.logging.PscLogger;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class MockServersetServiceDiscoveryProvider implements ServiceDiscoveryProvider {
    private static final PscLogger logger = PscLogger.getLogger(MockServersetServiceDiscoveryProvider.class);
    protected static String SERVERSET_DIR_PATH = "";

    @Override
    public ServiceDiscoveryConfig getConfig(Environment env, TopicUri topicUri) {
        return configureServiceDiscoveryConfig(env, topicUri);
    }

    protected ServiceDiscoveryConfig configureServiceDiscoveryConfig(Environment env,
                                                                     TopicUri topicUri) {
        String serversetFilename;
        try {
            serversetFilename = getServersetFilename(env, topicUri);
        } catch (ServiceDiscoveryException serviceDiscoveryException) {
            logger.info(serviceDiscoveryException.getMessage());
            return null;
        } catch (Exception e) {
            logger.warn("Failed to get serverset filename.", e);
            return null;
        }
        ServiceDiscoveryConfig discoveryConfig = new ServiceDiscoveryConfig();
        try {
            String connectString = getConnectionString(serversetFilename);
            discoveryConfig.setServiceDiscoveryProvider(this).setConnect(connectString);
        } catch (ServiceDiscoveryException exception) {
            logger.warn(
                    "Could not find serverset file {}. This is expected if app is not running on an EC2 host.",
                    serversetFilename, exception);
            return null;
        }
        return discoveryConfig;
    }

    protected abstract String getServersetFilename(Environment env, TopicUri uri) throws Exception;

    protected abstract String getServersetFilename(Environment environment,
                                                   String deploymentStage,
                                                   String backend,
                                                   String region,
                                                   String cluster,
                                                   String transportProtocol) throws ServiceDiscoveryException;

    protected String getConnectionString(String serversetFilename) throws ServiceDiscoveryException {
        List<String> endpoints = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(SERVERSET_DIR_PATH + serversetFilename))) {
            stream.forEach(endpoints::add);
        } catch (Exception exception) {
            throw new ServiceDiscoveryException("Could not find or read from file " + SERVERSET_DIR_PATH + serversetFilename,
                    exception);
        }

        Collections.shuffle(endpoints);

        return endpoints.subList(0, Math.min(endpoints.size(), 5)).stream().map(endpoint -> {
            String[] ipAndPort = endpoint.split(":");
            try {
                return Inet4Address.getByName(ipAndPort[0]).getHostName() + ":" + ipAndPort[1];
            } catch (UnknownHostException e) {
                return "";
            }
        }).filter(host -> !host.isEmpty()).collect(Collectors.joining(","));
    }

    @VisibleForTesting
    protected static void overrideServersetPath(String newPath) {
        SERVERSET_DIR_PATH = newPath;
    }
}
