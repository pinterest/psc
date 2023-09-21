package com.pinterest.psc.consumer.creation;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.logging.PscLogger;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("rawtypes")
public class PscConsumerCreatorManager {
    private static final PscLogger logger = PscLogger.getLogger(PscConsumerCreatorManager.class);
    private final Map<String, PscBackendConsumerCreator> backendCreatorMap =
            findAndRegisterConsumerCreators(PscConsumerCreatorManager.class.getPackage().getName());

    public static Map<String, PscBackendConsumerCreator> findAndRegisterConsumerCreators(String packageName) {
        synchronized (PscUtils.lock) {
            Map<String, PscBackendConsumerCreator> backendCreatorRegistry = new HashMap<>();
            Reflections reflections = new Reflections(packageName.trim());
            Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(PscConsumerCreatorPlugin.class);
            for (Class<?> annotatedClass : annotatedClasses) {
                PscConsumerCreatorPlugin plugin = annotatedClass.getAnnotation(PscConsumerCreatorPlugin.class);
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
                    logger.error("Output plugin alias '{}' already exists: {}", backend, annotatedClass.getName());
                    System.exit(-1);
                }
                try {
                    backendCreatorRegistry.put(backend, (PscBackendConsumerCreator) annotatedClass.newInstance());
                } catch (IllegalAccessException | InstantiationException e) {
                    throw new RuntimeException();
                }
                logger.info("Registered output handler ({}) with alias: {}", annotatedClass.getName(), backend);
            }
            return backendCreatorRegistry;
        }
    }

    public Map<String, PscBackendConsumerCreator> getBackendCreators() {
        return backendCreatorMap;
    }

    public void reset() {
        backendCreatorMap.values().forEach(PscBackendConsumerCreator::reset);
    }
}
