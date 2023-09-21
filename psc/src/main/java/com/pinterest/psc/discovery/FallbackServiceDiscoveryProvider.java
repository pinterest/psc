package com.pinterest.psc.discovery;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.logging.PscLogger;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * This service discovery provider uses a yaml file to provide service discovery configurations
 * the format of the yaml file is:
 * ---
 * <pre>{@code
 * <topic uri (uri of cluster for Kafka)>:
 *   connect: <String>
 *   securityProtocol: <Boolean>
 *   secureConfigs: (optional map for security configs)
 * }</pre>
 * ---
 * An example:
 * ---
 * <pre>{@code
 * plaintext:/rn:kafka:env:cloud_region::cluster:
 *   connect: broker1:9092,broker2:9092
 *   securityProtocol: PLAINTEXT
 * }</pre>
 * ---
 * <p>
 * The location of the file defaults to the working directory, but can be set with the system property "dev.discovery.config.file"
 */
@ServiceDiscoveryPlugin(priority = 0)
public class FallbackServiceDiscoveryProvider implements ServiceDiscoveryProvider {
    private static final PscLogger logger = PscLogger.getLogger(FallbackServiceDiscoveryProvider.class);
    private static final String DEFAULT_DISCOVERY_FILE = "discovery.json";
    private String fallbackDiscoveryConfigFile;

    public FallbackServiceDiscoveryProvider() {
    }

    @VisibleForTesting
    public FallbackServiceDiscoveryProvider(String fallbackFile) {
        fallbackDiscoveryConfigFile = fallbackFile;
    }

    @Override
    public void configure(PscConfiguration pscConfiguration) {
        fallbackDiscoveryConfigFile = pscConfiguration.getString(PscConfigurationInternal.getPscDiscoveryServiceProviderFallbackConfigName());
    }

    private InputStreamReader getFallbackDiscoveryFileAsReader() throws FileNotFoundException {
        if (fallbackDiscoveryConfigFile == null || fallbackDiscoveryConfigFile.trim().isEmpty()) {
            logger.info("No fallback service discovery file was provided in PSC configuration; " +
                    "checking for default file.");
            return getFallbackDiscoveryFileAsReader(null);
        } else if (Files.notExists(Paths.get(fallbackDiscoveryConfigFile))) {
            logger.warn("The configured service discovery file ({}) was not found; checking for default file.",
                    fallbackDiscoveryConfigFile
            );
            return getFallbackDiscoveryFileAsReader(null);
        }
        return getFallbackDiscoveryFileAsReader(fallbackDiscoveryConfigFile);
    }

    private InputStreamReader getFallbackDiscoveryFileAsReader(String filePath) throws FileNotFoundException {
        if (filePath == null) {
            // default case: use built-in discovery file
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_DISCOVERY_FILE);
            if (inputStream == null)
                throw new FileNotFoundException("Default discovery file could not be found: " + DEFAULT_DISCOVERY_FILE);
            logger.info("Loading discovery info from default fallback discovery file: {}", DEFAULT_DISCOVERY_FILE);
            return new InputStreamReader(inputStream);
        } else {
            logger.info("Loading discovery info from fallback discovery file: {}", fallbackDiscoveryConfigFile);
            return new FileReader(fallbackDiscoveryConfigFile);
        }
    }

    @Override
    public ServiceDiscoveryConfig getConfig(Environment env, TopicUri topicUri) {
        try (InputStreamReader reader = getFallbackDiscoveryFileAsReader()) {
            Gson gson = new Gson();
            Map<String, ServiceDiscoveryConfig> discoveryConfigMap =
                    gson.fromJson(reader, new TypeToken<Map<String, ServiceDiscoveryConfig>>() {
                    }.getType());
            ServiceDiscoveryConfig discoveryConfig = discoveryConfigMap.get(topicUri.getTopicUriPrefix());
            if (discoveryConfig == null) {
                logger.error("Failed to find service discovery config of uri {} in the discovery config file {}",
                        topicUri, fallbackDiscoveryConfigFile
                );
            }
            return discoveryConfig.setServiceDiscoveryProvider(this);
        } catch (FileNotFoundException fnfe) {
            logger.error("Failed to find {}", fallbackDiscoveryConfigFile);
            return null;
        } catch (IOException ioe) {
            logger.error("Failed to open the file " + fallbackDiscoveryConfigFile, ioe);
            return null;
        } catch (Exception exception) {
            logger.error("Unexpeted error", exception);
            return null;
        }
    }
}
