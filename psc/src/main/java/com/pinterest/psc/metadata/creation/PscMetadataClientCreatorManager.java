package com.pinterest.psc.metadata.creation;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.logging.PscLogger;
import org.reflections.Reflections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages the different {@link PscBackendMetadataClientCreator} implementations and provides a registry of them.
 *
 * This class is responsible for finding and registering the different {@link PscBackendMetadataClientCreator} implementations
 * that are annotated with {@link PscMetadataClientCreatorPlugin}. Each backend can have one or more implementations of
 * {@link PscBackendMetadataClientCreator} that are returned by this class, ordered by the plugin priority (descending) within
 * each backend. To access all the backend creators, keyed by the backend name, use {@link #getBackendCreators()}.
 */
public class PscMetadataClientCreatorManager {

    private static final PscLogger logger = PscLogger.getLogger(PscMetadataClientCreatorManager.class);
    private final Map<String, List<PscBackendMetadataClientCreator>> backendMetadataClientCreatorMap =
            findAndRegisterMetadataClientCreators(PscMetadataClientCreatorManager.class.getPackage().getName());

    private static Map<String, List<PscBackendMetadataClientCreator>> findAndRegisterMetadataClientCreators(String packageName) {
        synchronized (PscUtils.lock) {
            Map<String, List<PscBackendMetadataClientCreator>> backendCreatorRegistry = new HashMap<>();
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
                try {
                    PscBackendMetadataClientCreator creator =
                            (PscBackendMetadataClientCreator) annotatedClass.getDeclaredConstructor().newInstance();
                    List<PscBackendMetadataClientCreator> creators =
                            backendCreatorRegistry.computeIfAbsent(backend, ignored -> new ArrayList<>());
                    logger.info("Adding PscBackendMetadataClientCreator: {} with priority: {}", creator.getClass().getName(), getPriority(creator));
                    creators.add(creator);
                    creators.sort(Comparator.comparingInt(PscMetadataClientCreatorManager::getPriority));
                    Collections.reverse(creators); // Sort in descending order of priority
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException("Failed to register PscBackendMetadataClientCreator", e);
                }
            }
            backendCreatorRegistry.replaceAll((backend, creators) ->
                    Collections.unmodifiableList(new ArrayList<>(creators)));
            return Collections.unmodifiableMap(backendCreatorRegistry);
        }
    }

    private static int getPriority(PscBackendMetadataClientCreator creator) {
        PscMetadataClientCreatorPlugin annotation = creator.getClass().getAnnotation(PscMetadataClientCreatorPlugin.class);
        if (annotation == null) {
            logger.warn("Creator missing PscMetadataClientCreatorPlugin annotation; defaulting priority to Integer.MIN_VALUE: "
                    + creator.getClass().getName());
            return Integer.MIN_VALUE;
        }
        return annotation.priority();
    }

    public Map<String, List<PscBackendMetadataClientCreator>> getBackendCreators() {
        return backendMetadataClientCreatorMap;
    }
}
