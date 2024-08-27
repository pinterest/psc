package com.pinterest.psc.metadata.creation;

import com.pinterest.psc.logging.PscLogger;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PscMetadataClientCreatorManager {

    private static final PscLogger logger = PscLogger.getLogger(PscMetadataClientCreatorManager.class);
    private final Map<String, PscBackendMetadataClientCreator> backendMetadataClientCreatorMap =
            findAndRegisterMetadataClientCreators(PscMetadataClientCreatorManager.class.getPackage().getName());

    private static Map<String, PscBackendMetadataClientCreator> findAndRegisterMetadataClientCreators(String packageName) {
        Map<String, PscBackendMetadataClientCreator> backendCreatorRegistry = new HashMap<>();
        Reflections reflections = new Reflections(packageName.trim());
        Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(PscMetadataClientCreatorPlugin.class);
        for (Class<?> annotatedClass : annotatedClasses) {
            PscMetadataClientCreatorPlugin plugin = annotatedClass.getAnnotation(PscMetadataClientCreatorPlugin.class);
            if (plugin == null) {
                logger.error("Plugin info null: " + annotatedClass.getName());
                continue;
            }
            String backend = plugin.backend();
            if (backend.isEmpty()) {
                logger.warn("Ignoring due to empty backend for plugin: " + annotatedClass.getName());
                continue;
            }
            if (backendCreatorRegistry.containsKey(backend)) {
                logger.error("Output plugin alias '" + backend + "' already exists: " + annotatedClass.getName());
                System.exit(-1);
            }
            try {
                backendCreatorRegistry.put(backend, (PscBackendMetadataClientCreator) annotatedClass.newInstance());
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException("Failed to register PscBackendMetadataClientCreator", e);
            }
        }
        return backendCreatorRegistry;
    }

    public Map<String, PscBackendMetadataClientCreator> getBackendCreators() {
        return backendMetadataClientCreatorMap;
    }
}
