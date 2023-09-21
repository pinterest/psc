package com.pinterest.psc.discovery;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;
import org.reflections.Reflections;

import java.util.Collections;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ServiceDiscoveryManager {
    private static final PscLogger logger = PscLogger.getLogger(ServiceDiscoveryManager.class);
    private static final ThreadLocal<SortedMap<Integer, ServiceDiscoveryProvider>> providerMap =
            ThreadLocal.withInitial(() ->
                    findAndRegisterServiceDiscoveryProviders(ServiceDiscoveryManager.class.getPackage().getName())
            );

    /**
     * Returns a {@link SortedMap} of {@link ServiceDiscoveryProvider}s sorted by priority in descending order.
     *
     * The parameter packageName is used in {@link Reflections} to find all the available ServiceDiscoveryProviders,
     * which should each be annotated with their respective priorities. See example in {@link FallbackServiceDiscoveryProvider}
     * on how this is done.
     *
     * @param packageName the packageName to find all available {@link ServiceDiscoveryProvider}s
     * @return {@link SortedMap} of {@link ServiceDiscoveryProvider}s in descending order of priority
     */
    public static SortedMap<Integer, ServiceDiscoveryProvider> findAndRegisterServiceDiscoveryProviders(String packageName) {
        SortedMap<Integer, ServiceDiscoveryProvider> providerRegistry = new TreeMap<>(Collections.reverseOrder());
        Reflections reflections = new Reflections(packageName.trim());
        Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(ServiceDiscoveryPlugin.class);
        for (Class<?> annotatedClass : annotatedClasses) {
            ServiceDiscoveryPlugin plugin = annotatedClass.getAnnotation(ServiceDiscoveryPlugin.class);
            if (plugin == null) {
                logger.error("Plugin info null: {}", annotatedClass.getName());
                continue;
            }
            int priority = plugin.priority();
            if (providerRegistry.containsKey(priority)) {
                logger.error("Output plugin priority {} already exists: {}", priority, annotatedClass.getName());
                System.exit(-1);
            }
            try {
                providerRegistry.put(priority, annotatedClass.asSubclass(ServiceDiscoveryProvider.class).newInstance());
                logger.info("Registered output handler({}) with priority={}", annotatedClass.getName(), priority);
            } catch (InstantiationException | IllegalAccessException e) {
                logger.error("Failed to instantiate service provider class " + annotatedClass.getName(), e);
            }
        }

        return providerRegistry;
    }

    public static ServiceDiscoveryConfig getServiceDiscoveryConfig(
            Environment env, PscConfiguration discoveryConfiguration, TopicUri topicUri
    ) throws ConfigurationException {
        for (ServiceDiscoveryProvider provider : providerMap.get().values()) {
            provider.configure(discoveryConfiguration);
            ServiceDiscoveryConfig config = provider.getConfig(env, topicUri);
            if (config != null) {
                logger.info("Using {} as the active discovery provider with config {}.",
                        provider.getClass().getName(), config
                );
                return config;
            }
        }
        throw new ConfigurationException("Could not generate service discovery config for " + topicUri);
    }

    @VisibleForTesting
    protected static SortedMap<Integer, ServiceDiscoveryProvider> getProviderMap() {
        return providerMap.get();
    }

    @VisibleForTesting
    protected static SortedMap<Integer, ServiceDiscoveryProvider> resetAndGetProviderMap() {
        providerMap.set(findAndRegisterServiceDiscoveryProviders(ServiceDiscoveryManager.class.getPackage().getName()));
        return providerMap.get();
    }
}
