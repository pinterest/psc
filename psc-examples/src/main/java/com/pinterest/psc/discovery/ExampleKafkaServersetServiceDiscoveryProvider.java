package com.pinterest.psc.discovery;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ServiceDiscoveryException;
import com.pinterest.psc.logging.PscLogger;

@ServiceDiscoveryPlugin(priority = 110)
public class ExampleKafkaServersetServiceDiscoveryProvider implements ServiceDiscoveryProvider {

    private static final PscLogger logger = PscLogger.getLogger(ExampleKafkaServersetServiceDiscoveryProvider.class);
    private static final String SERVERSET_DIR_PATH = System.getProperty("tempServersetDir");

    @Override
    public ServiceDiscoveryConfig getConfig(Environment env, TopicUri uri) {
        ServiceDiscoveryConfig discoveryConfig = configureServiceDiscoveryConfig(env, uri);
        return discoveryConfig;    
    }

    private ServiceDiscoveryConfig configureServiceDiscoveryConfig(Environment env, TopicUri topicUri) {
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
                    "Could not find serverset file {}.",
                    serversetFilename, exception);
            return null;
        }
        return discoveryConfig;
    }

    private String getServersetFilename(Environment env, TopicUri topicUri) throws ServiceDiscoveryException {
        if (topicUri instanceof KafkaTopicUri) {
            KafkaTopicUri kafkaUri = (KafkaTopicUri) topicUri;
            String region = kafkaUri.getRegion().toLowerCase();
            String cluster = kafkaUri.getCluster().toLowerCase();
            String transportProtocol = kafkaUri.getProtocol().toLowerCase();

            return String.format("%s.discovery.%s%s.serverset",
                    region,
                    cluster,
                    (transportProtocol.equals(KafkaTopicUri.SECURE_PROTOCOL)) ? "_tls" : ""
            );
        } else {
            throw new ServiceDiscoveryException(topicUri + " is not an KafkaTopicUri instance");
        }
    }

    private String getConnectionString(String serversetFilename) throws ServiceDiscoveryException {
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
    
}
