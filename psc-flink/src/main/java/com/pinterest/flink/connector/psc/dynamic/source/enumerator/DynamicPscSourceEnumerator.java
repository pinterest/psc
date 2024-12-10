/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.dynamic.source.enumerator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.dynamic.source.DynamicPscSourceOptions;
import com.pinterest.flink.connector.psc.dynamic.source.GetMetadataUpdateEvent;
import com.pinterest.flink.connector.psc.dynamic.source.MetadataUpdateEvent;
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.subscriber.PscStreamSubscriber;
import com.pinterest.flink.connector.psc.dynamic.source.split.DynamicPscSourceSplit;
import com.pinterest.flink.connector.psc.source.PscPropertiesUtil;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumState;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumerator;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriber;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This enumerator manages multiple {@link PscSourceEnumerator}'s, which does not have any
 * synchronization since it assumes single threaded execution.
 */
@Internal
public class DynamicPscSourceEnumerator
        implements SplitEnumerator<DynamicPscSourceSplit, DynamicPscSourceEnumState> {
    private static final Logger logger =
            LoggerFactory.getLogger(DynamicPscSourceEnumerator.class);

    // Each cluster will have its own sub enumerator
    private final Map<String, SplitEnumerator<PscTopicUriPartitionSplit, PscSourceEnumState>>
            clusterEnumeratorMap;

    // The mapping that the split enumerator context needs to be able to forward certain requests.
    private final Map<String, StoppablePscEnumContextProxy> clusterEnumContextMap;
    private final PscStreamSubscriber pscStreamSubscriber;
    private final SplitEnumeratorContext<DynamicPscSourceSplit> enumContext;
    private final PscMetadataService pscMetadataService;
    private final Properties properties;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final Boundedness boundedness;
    private final StoppablePscEnumContextProxy.StoppablePscEnumContextProxyFactory
            stoppablePscEnumContextProxyFactory;

    // options
    private final long pscMetadataServiceDiscoveryIntervalMs;
    private final int pscMetadataServiceDiscoveryFailureThreshold;

    // state
    private int pscMetadataServiceDiscoveryFailureCount;
    private Map<String, Set<String>> latestClusterTopicsMap;
    private Set<PscStream> latestPscStreams;
    private boolean firstDiscoveryComplete;

    public DynamicPscSourceEnumerator(
            PscStreamSubscriber pscStreamSubscriber,
            PscMetadataService pscMetadataService,
            SplitEnumeratorContext<DynamicPscSourceSplit> enumContext,
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            Boundedness boundedness,
            DynamicPscSourceEnumState dynamicPscSourceEnumState) {
        this(
                pscStreamSubscriber,
                pscMetadataService,
                enumContext,
                startingOffsetsInitializer,
                stoppingOffsetInitializer,
                properties,
                boundedness,
                dynamicPscSourceEnumState,
                StoppablePscEnumContextProxy.StoppablePscEnumContextProxyFactory
                        .getDefaultFactory());
    }

    @VisibleForTesting
    DynamicPscSourceEnumerator(
            PscStreamSubscriber pscStreamSubscriber,
            PscMetadataService pscMetadataService,
            SplitEnumeratorContext<DynamicPscSourceSplit> enumContext,
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            Boundedness boundedness,
            DynamicPscSourceEnumState dynamicPscSourceEnumState,
            StoppablePscEnumContextProxy.StoppablePscEnumContextProxyFactory
                    stoppablePscEnumContextProxyFactory) {
        this.pscStreamSubscriber = pscStreamSubscriber;
        this.boundedness = boundedness;

        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.properties = properties;
        this.enumContext = enumContext;

        // options
        this.pscMetadataServiceDiscoveryIntervalMs =
                DynamicPscSourceOptions.getOption(
                        properties,
                        DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);
        this.pscMetadataServiceDiscoveryFailureThreshold =
                DynamicPscSourceOptions.getOption(
                        properties,
                        DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD,
                        Integer::parseInt);
        this.pscMetadataServiceDiscoveryFailureCount = 0;
        this.firstDiscoveryComplete = false;

        this.pscMetadataService = pscMetadataService;
        this.stoppablePscEnumContextProxyFactory = stoppablePscEnumContextProxyFactory;

        // handle checkpoint state and rebuild contexts
        this.clusterEnumeratorMap = new HashMap<>();
        this.clusterEnumContextMap = new HashMap<>();
        this.latestPscStreams = dynamicPscSourceEnumState.getPscStreams();

        Map<String, Properties> clusterProperties = new HashMap<>();
        for (PscStream pscStream : latestPscStreams) {
            for (Entry<String, ClusterMetadata> entry :
                    pscStream.getClusterMetadataMap().entrySet()) {
                clusterProperties.put(entry.getKey(), entry.getValue().getProperties());
            }
        }

        this.latestClusterTopicsMap = new HashMap<>();
        for (Entry<String, PscSourceEnumState> clusterEnumState :
                dynamicPscSourceEnumState.getClusterEnumeratorStates().entrySet()) {
            this.latestClusterTopicsMap.put(
                    clusterEnumState.getKey(),
                    clusterEnumState.getValue().assignedPartitions().stream()
                            .map(TopicUriPartition::getTopicUri)
                            .map(TopicUri::getTopicUriAsString)
                            .collect(Collectors.toSet()));

            createEnumeratorWithAssignedTopicUriPartitions(
                    clusterEnumState.getKey(),
                    this.latestClusterTopicsMap.get(clusterEnumState.getKey()),
                    clusterEnumState.getValue(),
                    clusterProperties.get(clusterEnumState.getKey()));
        }
    }

    /**
     * Discover Kafka clusters and initialize sub enumerators. Bypass kafka metadata service
     * discovery if there exists prior state. Exceptions with initializing Kafka source are treated
     * the same as Kafka state and metadata inconsistency.
     */
    @Override
    public void start() {
        // if there is checkpoint state, start all enumerators first.
        if (!clusterEnumeratorMap.isEmpty()) {
            startAllEnumerators();
        }

        if (pscMetadataServiceDiscoveryIntervalMs <= 0) {
            enumContext.callAsync(
                    () -> pscStreamSubscriber.getSubscribedStreams(pscMetadataService),
                    this::onHandleSubscribedStreamsFetch);
        } else {
            enumContext.callAsync(
                    () -> pscStreamSubscriber.getSubscribedStreams(pscMetadataService),
                    this::onHandleSubscribedStreamsFetch,
                    0,
                    pscMetadataServiceDiscoveryIntervalMs);
        }
    }

    private void handleNoMoreSplits() {
        if (Boundedness.BOUNDED.equals(boundedness)) {
            boolean allEnumeratorsHaveSignalledNoMoreSplits = true;
            for (StoppablePscEnumContextProxy context : clusterEnumContextMap.values()) {
                allEnumeratorsHaveSignalledNoMoreSplits =
                        allEnumeratorsHaveSignalledNoMoreSplits && context.isNoMoreSplits();
            }

            if (firstDiscoveryComplete && allEnumeratorsHaveSignalledNoMoreSplits) {
                logger.info(
                        "Signal no more splits to all readers: {}",
                        enumContext.registeredReaders().keySet());
                enumContext.registeredReaders().keySet().forEach(enumContext::signalNoMoreSplits);
            } else {
                logger.info("Not ready to notify no more splits to readers.");
            }
        }
    }

    // --------------- private methods for metadata discovery ---------------

    private void onHandleSubscribedStreamsFetch(Set<PscStream> fetchedPscStreams, Throwable t) {
        firstDiscoveryComplete = true;
        Set<PscStream> handledFetchPscStreams =
                handleFetchSubscribedStreamsError(fetchedPscStreams, t);

        Map<String, Set<String>> newClustersToTopicUrisMap = new HashMap<>();
        Map<String, Properties> clusterProperties = new HashMap<>();
        for (PscStream pscStream : handledFetchPscStreams) {
            for (Entry<String, ClusterMetadata> entry :
                    pscStream.getClusterMetadataMap().entrySet()) {
                String clusterId = entry.getKey();
                ClusterMetadata clusterMetadata = entry.getValue();

                newClustersToTopicUrisMap
                        .computeIfAbsent(clusterId, (unused) -> new HashSet<>())
                        .addAll(clusterMetadata.getTopicUris());
                clusterProperties.put(clusterId, clusterMetadata.getProperties());
            }
        }

        // don't do anything if no change
        if (latestClusterTopicsMap.equals(newClustersToTopicUrisMap)) {
            return;
        }

        if (logger.isInfoEnabled()) {
            MapDifference<String, Set<String>> metadataDifference =
                    Maps.difference(latestClusterTopicsMap, newClustersToTopicUrisMap);
            logger.info(
                    "Common cluster topics after metadata refresh: {}",
                    metadataDifference.entriesInCommon());
            logger.info(
                    "Removed cluster topics after metadata refresh: {}",
                    metadataDifference.entriesOnlyOnLeft());
            logger.info(
                    "Additional cluster topics after metadata refresh: {}",
                    metadataDifference.entriesOnlyOnRight());
        }

        DynamicPscSourceEnumState dynamicPscSourceEnumState;
        try {
            dynamicPscSourceEnumState = snapshotState(-1);
        } catch (Exception e) {
            throw new RuntimeException("unable to snapshot state in metadata change", e);
        }

        logger.info("Closing enumerators due to metadata change");

        closeAllEnumeratorsAndContexts();
        latestClusterTopicsMap = newClustersToTopicUrisMap;
        latestPscStreams = handledFetchPscStreams;
        sendMetadataUpdateEventToAvailableReaders();

        // create enumerators
        for (Entry<String, Set<String>> activeClusterTopics : latestClusterTopicsMap.entrySet()) {
            final Set<TopicUriPartition> activeTopicPartitions = new HashSet<>();

            if (dynamicPscSourceEnumState
                            .getClusterEnumeratorStates()
                            .get(activeClusterTopics.getKey())
                    != null) {
                Set<TopicUriPartition> oldTopicPartitions =
                        dynamicPscSourceEnumState
                                .getClusterEnumeratorStates()
                                .get(activeClusterTopics.getKey())
                                .assignedPartitions();
                // filter out removed topics
                for (TopicUriPartition oldTopicPartition : oldTopicPartitions) {
                    if (activeClusterTopics.getValue().contains(oldTopicPartition.getTopicUri().getTopic())) {
                        activeTopicPartitions.add(oldTopicPartition);
                    }
                }
            }

            // restarts enumerator from state using only the active topic partitions, to avoid
            // sending duplicate splits from enumerator
            createEnumeratorWithAssignedTopicUriPartitions(
                    activeClusterTopics.getKey(),
                    activeClusterTopics.getValue(),
                    dynamicPscSourceEnumState
                            .getClusterEnumeratorStates()
                            .getOrDefault(
                                    activeClusterTopics.getKey(),
                                    new PscSourceEnumState(
                                            Collections.emptySet(), Collections.emptySet(), false)),
                    clusterProperties.get(activeClusterTopics.getKey()));
        }

        startAllEnumerators();
    }

    private Set<PscStream> handleFetchSubscribedStreamsError(
            Set<PscStream> fetchedPscStreams, @Nullable Throwable t) {
        if (t != null) {
            if (!latestPscStreams.isEmpty()
                    && ++pscMetadataServiceDiscoveryFailureCount
                            <= pscMetadataServiceDiscoveryFailureThreshold) {
                logger.warn("Swallowing metadata service error", t);
                // reuse state
                return latestPscStreams;
            } else {
                throw new RuntimeException(
                        "Fetching subscribed Kafka streams failed and no metadata to fallback", t);
            }
        } else {
            // reset count in absence of failure
            pscMetadataServiceDiscoveryFailureCount = 0;
            return fetchedPscStreams;
        }
    }

    /** NOTE: Must run on coordinator thread. */
    private void sendMetadataUpdateEventToAvailableReaders() {
        for (int readerId : enumContext.registeredReaders().keySet()) {
            MetadataUpdateEvent metadataUpdateEvent = new MetadataUpdateEvent(latestPscStreams);
            logger.info("sending metadata update to reader {}: {}", readerId, metadataUpdateEvent);
            enumContext.sendEventToSourceReader(readerId, metadataUpdateEvent);
        }
    }

    /**
     * Initialize KafkaEnumerators, maybe with the topic partitions that are already assigned to by
     * readers, to avoid duplicate re-assignment of splits. This is especially important in the
     * restart mechanism when duplicate split assignment can cause undesired starting offsets (e.g.
     * not assigning to the offsets prior to reader restart). Split offset resolution is mostly
     * managed by the readers.
     *
     * <p>NOTE: Must run on coordinator thread
     */
    private PscSourceEnumerator createEnumeratorWithAssignedTopicUriPartitions(
            String clusterId,
            Set<String> topicUris,
            PscSourceEnumState pscSourceEnumState,
            Properties fetchedProperties) {
        final Runnable signalNoMoreSplitsCallback;
        if (Boundedness.BOUNDED.equals(boundedness)) {
            signalNoMoreSplitsCallback = this::handleNoMoreSplits;
        } else {
            signalNoMoreSplitsCallback = null;
        }

        StoppablePscEnumContextProxy context =
                stoppablePscEnumContextProxyFactory.create(
                        enumContext,
                        clusterId,
                        pscMetadataService,
                        signalNoMoreSplitsCallback);

        Properties consumerProps = new Properties();
        PscPropertiesUtil.copyProperties(fetchedProperties, consumerProps);
        PscPropertiesUtil.copyProperties(properties, consumerProps);
        PscPropertiesUtil.setClientIdPrefix(consumerProps, clusterId);

        System.out.println("fetchedProperties: " + fetchedProperties);
        Thread.dumpStack();

        PscSourceEnumerator enumerator =
                new PscSourceEnumerator(
                        PscSubscriber.getTopicUriListSubscriber(new ArrayList<>(topicUris)),
                        startingOffsetsInitializer,
                        stoppingOffsetInitializer,
                        consumerProps,
                        context,
                        boundedness,
                        pscSourceEnumState);

        clusterEnumContextMap.put(clusterId, context);
        clusterEnumeratorMap.put(clusterId, enumerator);

        return enumerator;
    }

    private void startAllEnumerators() {
        for (String kafkaClusterId : latestClusterTopicsMap.keySet()) {
            try {
                // starts enumerators and handles split discovery and assignment
                clusterEnumeratorMap.get(kafkaClusterId).start();
            } catch (Exception e) {
                if (pscMetadataService.isClusterActive(kafkaClusterId)) {
                    throw new RuntimeException(
                            String.format("Failed to create enumerator for %s", kafkaClusterId), e);
                } else {
                    logger.info(
                            "Found inactive cluster {} while initializing, removing enumerator",
                            kafkaClusterId,
                            e);
                    try {
                        clusterEnumContextMap.remove(kafkaClusterId).close();
                        clusterEnumeratorMap.remove(kafkaClusterId).close();
                    } catch (Exception ex) {
                        // closing enumerator throws an exception, let error propagate and restart
                        // the job
                        throw new RuntimeException(
                                "Failed to close enum context for " + kafkaClusterId, ex);
                    }
                }
            }
        }
    }

    private void closeAllEnumeratorsAndContexts() {
        clusterEnumeratorMap.forEach(
                (cluster, subEnumerator) -> {
                    try {
                        clusterEnumContextMap.get(cluster).close();
                        subEnumerator.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        clusterEnumContextMap.clear();
        clusterEnumeratorMap.clear();
    }

    /**
     * Multi cluster Kafka source readers will not request splits. Splits will be pushed to them,
     * similarly for the sub enumerators.
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        throw new UnsupportedOperationException("Kafka enumerators only assign splits to readers.");
    }

    @Override
    public void addSplitsBack(List<DynamicPscSourceSplit> splits, int subtaskId) {
        logger.debug("Adding splits back for {}", subtaskId);
        // separate splits by cluster
        ArrayListMultimap<String, PscTopicUriPartitionSplit> kafkaPartitionSplits =
                ArrayListMultimap.create();
        for (DynamicPscSourceSplit split : splits) {
            kafkaPartitionSplits.put(split.getPubSubClusterId(), split.getPscTopicUriPartitionSplit());
        }

        // add splits back and assign pending splits for all enumerators
        for (String kafkaClusterId : kafkaPartitionSplits.keySet()) {
            if (clusterEnumeratorMap.containsKey(kafkaClusterId)) {
                clusterEnumeratorMap
                        .get(kafkaClusterId)
                        .addSplitsBack(kafkaPartitionSplits.get(kafkaClusterId), subtaskId);
            } else {
                logger.warn(
                        "Split refers to inactive cluster {} with current clusters being {}",
                        kafkaClusterId,
                        clusterEnumeratorMap.keySet());
            }
        }

        handleNoMoreSplits();
    }

    /** NOTE: this happens at startup and failover. */
    @Override
    public void addReader(int subtaskId) {
        logger.debug("Adding reader {}", subtaskId);
        // assign pending splits from the sub enumerator
        clusterEnumeratorMap.forEach(
                (cluster, subEnumerator) -> subEnumerator.addReader(subtaskId));
        handleNoMoreSplits();
    }

    /**
     * Besides for checkpointing, this method is used in the restart sequence to retain the relevant
     * assigned splits so that there is no reader duplicate split assignment. See {@link
     * #createEnumeratorWithAssignedTopicUriPartitions(String, Set, PscSourceEnumState, Properties)}.
     */
    @Override
    public DynamicPscSourceEnumState snapshotState(long checkpointId) throws Exception {
        Map<String, PscSourceEnumState> subEnumeratorStateByCluster = new HashMap<>();

        // populate map for all assigned splits
        for (Entry<String, SplitEnumerator<PscTopicUriPartitionSplit, PscSourceEnumState>>
                clusterEnumerator : clusterEnumeratorMap.entrySet()) {
            subEnumeratorStateByCluster.put(
                    clusterEnumerator.getKey(),
                    clusterEnumerator.getValue().snapshotState(checkpointId));
        }

        return new DynamicPscSourceEnumState(latestPscStreams, subEnumeratorStateByCluster);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        Preconditions.checkArgument(
                sourceEvent instanceof GetMetadataUpdateEvent,
                "Received invalid source event: " + sourceEvent);

        if (enumContext.registeredReaders().containsKey(subtaskId)) {
            MetadataUpdateEvent metadataUpdateEvent = new MetadataUpdateEvent(latestPscStreams);
            logger.info("sending metadata update to reader {}: {}", subtaskId, metadataUpdateEvent);
            enumContext.sendEventToSourceReader(subtaskId, metadataUpdateEvent);
        } else {
            logger.warn("Got get metadata update but subtask was unavailable");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // close contexts first since they may have running tasks
            for (StoppablePscEnumContextProxy subEnumContext : clusterEnumContextMap.values()) {
                subEnumContext.close();
            }

            for (Entry<String, SplitEnumerator<PscTopicUriPartitionSplit, PscSourceEnumState>>
                    clusterEnumerator : clusterEnumeratorMap.entrySet()) {
                clusterEnumerator.getValue().close();
            }

            pscMetadataService.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
