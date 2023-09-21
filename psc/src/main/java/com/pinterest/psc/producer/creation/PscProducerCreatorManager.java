package com.pinterest.psc.producer.creation;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.logging.PscLogger;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PscProducerCreatorManager {
    private static final PscLogger logger = PscLogger.getLogger(PscProducerCreatorManager.class);
    private final Map<String, PscBackendProducerCreator> backendCreatorMap =
            findAndRegisterProducerCreators(PscProducerCreatorManager.class.getPackage().getName());

    public static Map<String, PscBackendProducerCreator> findAndRegisterProducerCreators(String packageName) {
        synchronized (PscUtils.lock) {
            Map<String, PscBackendProducerCreator> backendCreatorRegistry = new HashMap<>();
            Reflections reflections = new Reflections(packageName.trim());
            Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(PscProducerCreatorPlugin.class);
            for (Class<?> annotatedClass : annotatedClasses) {
                PscProducerCreatorPlugin plugin = annotatedClass.getAnnotation(PscProducerCreatorPlugin.class);
                if (plugin == null) {
                    logger.error("Plugin info null: {}", annotatedClass.getName());
                    continue;
                }
                String backend = plugin.backend();
                if (backend.isEmpty()) {
                    logger.warn("Ignoring aggregation function: {}", annotatedClass.getName());
                    continue;
                }
                if (backendCreatorRegistry.containsKey(backend)) {
                    logger.error(
                            "Output plugin alias '{}' already exists: {}", backend, annotatedClass.getName());
                    System.exit(-1);
                }
                try {
                    backendCreatorRegistry.put(backend, (PscBackendProducerCreator) annotatedClass.newInstance());
                } catch (IllegalAccessException | InstantiationException e) {
                    throw new RuntimeException();
                }
                logger.info("Registered output handler ({}) with alias: {}", annotatedClass.getName(), backend);
            }
            return backendCreatorRegistry;
        }
    }

    public Map<String, PscBackendProducerCreator> getBackendCreators() {
        return backendCreatorMap;
    }

    public void reset() {
        backendCreatorMap.values().forEach(PscBackendProducerCreator::reset);
    }
}