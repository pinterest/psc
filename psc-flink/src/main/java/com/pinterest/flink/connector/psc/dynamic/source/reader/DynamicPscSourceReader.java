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

package com.pinterest.flink.connector.psc.dynamic.source.reader;

import com.google.common.collect.ArrayListMultimap;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.dynamic.source.GetMetadataUpdateEvent;
import com.pinterest.flink.connector.psc.dynamic.source.MetadataUpdateEvent;
import com.pinterest.flink.connector.psc.dynamic.source.metrics.PscClusterMetricGroup;
import com.pinterest.flink.connector.psc.dynamic.source.metrics.PscClusterMetricGroupManager;
import com.pinterest.flink.connector.psc.dynamic.source.split.DynamicPscSourceSplit;
import com.pinterest.flink.connector.psc.source.PscPropertiesUtil;
import com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics;
import com.pinterest.flink.connector.psc.source.reader.PscRecordEmitter;
import com.pinterest.flink.connector.psc.source.reader.PscSourceReader;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.source.reader.fetcher.PscSourceFetcherManager;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Manages state about underlying {@link PscSourceReader} to collect records and commit offsets
 * from multiple Kafka clusters. This reader also handles changes to Kafka topology by reacting to
 * restart sequence initiated by the enumerator and suspending inconsistent sub readers.
 *
 * <p>First, in the restart sequence, we will receive the {@link com.pinterest.flink.connector.psc.dynamic.source.MetadataUpdateEvent} from the
 * enumerator, stop all KafkaSourceReaders, and retain the relevant splits. Second, enumerator will
 * send all new splits that readers should work on (old splits will not be sent again).
 */
@Internal
public class DynamicPscSourceReader<T> implements SourceReader<T, DynamicPscSourceSplit> {
    private static final Logger logger = LoggerFactory.getLogger(DynamicPscSourceReader.class);
    private final PscRecordDeserializationSchema<T> deserializationSchema;
    private final Properties properties;
    private final MetricGroup dynamicPscSourceMetricGroup;
    private final Gauge<Integer> pubsubClusterCount;
    private final SourceReaderContext readerContext;
    private final PscClusterMetricGroupManager pscClusterMetricGroupManager;

    // needs have a strict ordering for readers to guarantee availability future consistency
    private final NavigableMap<String, PscSourceReader<T>> clusterReaderMap;
    private final Map<String, Properties> clustersProperties;
    private final List<DynamicPscSourceSplit> pendingSplits;

    private MultipleFuturesAvailabilityHelper availabilityHelper;
    private boolean isActivelyConsumingSplits;
    private boolean isNoMoreSplits;
    private AtomicBoolean restartingReaders;

    public DynamicPscSourceReader(
            SourceReaderContext readerContext,
            PscRecordDeserializationSchema<T> deserializationSchema,
            Properties properties) {
        this.readerContext = readerContext;
        this.clusterReaderMap = new TreeMap<>();
        this.deserializationSchema = deserializationSchema;
        this.properties = properties;
        this.pubsubClusterCount = clusterReaderMap::size;
        this.dynamicPscSourceMetricGroup =
                readerContext
                        .metricGroup()
                        .addGroup(PscClusterMetricGroup.DYNAMIC_PSC_SOURCE_METRIC_GROUP);
        this.pscClusterMetricGroupManager = new PscClusterMetricGroupManager();
        this.pendingSplits = new ArrayList<>();
        this.availabilityHelper = new MultipleFuturesAvailabilityHelper(0);
        this.isNoMoreSplits = false;
        this.isActivelyConsumingSplits = false;
        this.restartingReaders = new AtomicBoolean();
        this.clustersProperties = new HashMap<>();
    }

    /**
     * This is invoked first only in reader startup without state. In stateful startup, splits are
     * added before this method is invoked.
     */
    @Override
    public void start() {
        logger.trace("Starting reader for subtask index={}", readerContext.getIndexOfSubtask());
        // metrics cannot be registered in the enumerator
        readerContext.metricGroup().gauge("kafkaClusterCount", pubsubClusterCount);
        readerContext.sendSourceEventToCoordinator(new GetMetadataUpdateEvent());
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> readerOutput) throws Exception {
        // at startup, do not return end of input if metadata event has not been received
        if (clusterReaderMap.isEmpty()) {
            return logAndReturnInputStatus(InputStatus.NOTHING_AVAILABLE);
        }

        if (restartingReaders.get()) {
            logger.info("Poll next invoked while restarting readers");
            return logAndReturnInputStatus(InputStatus.NOTHING_AVAILABLE);
        }

        boolean isMoreAvailable = false;
        boolean isNothingAvailable = false;

        for (Map.Entry<String, PscSourceReader<T>> clusterReader : clusterReaderMap.entrySet()) {
            InputStatus inputStatus = clusterReader.getValue().pollNext(readerOutput);
            switch (inputStatus) {
                case MORE_AVAILABLE:
                    isMoreAvailable = true;
                    break;
                case NOTHING_AVAILABLE:
                    isNothingAvailable = true;
                    break;
            }
        }

        return logAndReturnInputStatus(consolidateInputStatus(isMoreAvailable, isNothingAvailable));
    }

    private InputStatus consolidateInputStatus(
            boolean atLeastOneMoreAvailable, boolean atLeastOneNothingAvailable) {
        final InputStatus inputStatus;
        if (atLeastOneMoreAvailable) {
            inputStatus = InputStatus.MORE_AVAILABLE;
        } else if (atLeastOneNothingAvailable) {
            inputStatus = InputStatus.NOTHING_AVAILABLE;
        } else {
            inputStatus = InputStatus.END_OF_INPUT;
        }
        return inputStatus;
    }

    // we also need to filter splits at startup in case checkpoint is not consistent bwtn enumerator
    // and reader
    @Override
    public void addSplits(List<DynamicPscSourceSplit> splits) {
        logger.info("Adding splits to reader {}: {}", readerContext.getIndexOfSubtask(), splits);
        // at startup, don't add splits until we get confirmation from enumerator of the current
        // metadata
        if (!isActivelyConsumingSplits) {
            pendingSplits.addAll(splits);
            return;
        }

        ArrayListMultimap<String, PscTopicUriPartitionSplit> clusterSplitsMap =
                ArrayListMultimap.create();
        for (DynamicPscSourceSplit split : splits) {
            clusterSplitsMap.put(split.getClusterId(), split);
        }

        Set<String> clusterIds = clusterSplitsMap.keySet();

        boolean newCluster = false;
        for (String clusterId : clusterIds) {
            // if a reader corresponding to the split doesn't exist, create it
            // it is possible that the splits come before the source event
            if (!clusterReaderMap.containsKey(clusterId)) {
                try {
                    PscSourceReader<T> pscSourceReader = createReader(clusterId);
                    clusterReaderMap.put(clusterId, pscSourceReader);
                    pscSourceReader.start();
                    newCluster = true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            // add splits
            PscSourceReader<T> reader = clusterReaderMap.get(clusterId);
            reader.addSplits(clusterSplitsMap.get(clusterId));
        }

        // reset the availability future to also depend on the new sub readers
        if (newCluster) {
            completeAndResetAvailabilityHelper();
        }
    }

    /**
     * Duplicate source events are handled with idempotency. No metadata change means we simply skip
     * the restart logic.
     */
    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        Preconditions.checkArgument(
                sourceEvent instanceof MetadataUpdateEvent,
                "Received invalid source event: " + sourceEvent);

        logger.info(
                "Received source event {}: subtask={}",
                sourceEvent,
                readerContext.getIndexOfSubtask());
        Set<PscStream> newPscStreams = ((MetadataUpdateEvent) sourceEvent).getPscStreams();
        Map<String, Set<String>> newClustersAndTopicUris = new HashMap<>();
        Map<String, Properties> newClustersProperties = new HashMap<>();
        for (PscStream kafkaStream : newPscStreams) {
            for (Map.Entry<String, ClusterMetadata> clusterMetadataMapEntry :
                    kafkaStream.getClusterMetadataMap().entrySet()) {
                newClustersAndTopicUris
                        .computeIfAbsent(
                                clusterMetadataMapEntry.getKey(), (unused) -> new HashSet<>())
                        .addAll(clusterMetadataMapEntry.getValue().getTopicUris());

                newClustersProperties.put(
                        clusterMetadataMapEntry.getKey(),
                        clusterMetadataMapEntry.getValue().getProperties());
            }
        }

        // filter current splits with the metadata update
        List<DynamicPscSourceSplit> currentSplitState = snapshotStateFromAllReaders(-1);
        logger.info(
                "Snapshotting split state for reader {}: {}",
                readerContext.getIndexOfSubtask(),
                currentSplitState);
        Map<String, Set<String>> currentMetadataFromState = new HashMap<>();
        Map<String, List<PscTopicUriPartitionSplit>> filteredNewClusterSplitStateMap = new HashMap<>();

        // the data structures above
        for (DynamicPscSourceSplit split : currentSplitState) {
            currentMetadataFromState
                    .computeIfAbsent(split.getClusterId(), (ignore) -> new HashSet<>())
                    .add(split.getPscTopicUriPartitionSplit().getTopic());
            // check if cluster topic exists in the metadata update
            if (newClustersAndTopicUris.containsKey(split.getClusterId())
                    && newClustersAndTopicUris
                            .get(split.getClusterId())
                            .contains(split.getPscTopicUriPartitionSplit().getTopic())) {
                filteredNewClusterSplitStateMap
                        .computeIfAbsent(split.getClusterId(), (ignore) -> new ArrayList<>())
                        .add(split);
            } else {
                logger.info("Skipping outdated split due to metadata changes: {}", split);
            }
        }

        // only restart if there was metadata change to handle duplicate MetadataUpdateEvent from
        // enumerator. We can possibly only restart the readers whose metadata has changed but that
        // comes at the cost of complexity and it is an optimization for a corner case. We can
        // revisit if necessary.
        if (!newClustersAndTopicUris.equals(currentMetadataFromState)) {
            restartingReaders.set(true);
            closeAllReadersAndClearState();

            clustersProperties.putAll(newClustersProperties);
            for (String clusterId : newClustersAndTopicUris.keySet()) {
                try {
                    // restart kafka source readers with the relevant state
                    PscSourceReader<T> pscSourceReader = createReader(clusterId);
                    clusterReaderMap.put(clusterId, pscSourceReader);
                    if (filteredNewClusterSplitStateMap.containsKey(clusterId)) {
                        pscSourceReader.addSplits(
                                filteredNewClusterSplitStateMap.get(clusterId));
                    }
                    pscSourceReader.start();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            // reset the availability future to also depend on the new sub readers
            completeAndResetAvailabilityHelper();
        } else {
            // update properties even on no metadata change
            clustersProperties.clear();
            clustersProperties.putAll(newClustersProperties);
        }

        // finally mark the reader as active, if not already and add pending splits
        if (!isActivelyConsumingSplits) {
            isActivelyConsumingSplits = true;
        }

        if (!pendingSplits.isEmpty()) {
            List<DynamicPscSourceSplit> validPendingSplits =
                    pendingSplits.stream()
                            // Pending splits is used to cache splits at startup, before metadata
                            // update event arrives. Splits in state could be old and it's possible
                            // to not have another metadata update event, so need to filter the
                            // splits at this point.
                            .filter(
                                    pendingSplit -> {
                                        boolean splitValid =
                                                isSplitForActiveClusters(
                                                        pendingSplit, newClustersAndTopicUris);
                                        if (!splitValid) {
                                            logger.info(
                                                    "Removing invalid split for reader: {}",
                                                    pendingSplit);
                                        }
                                        return splitValid;
                                    })
                            .collect(Collectors.toList());

            addSplits(validPendingSplits);
            pendingSplits.clear();
            if (isNoMoreSplits) {
                notifyNoMoreSplits();
            }
        }
    }

    private static boolean isSplitForActiveClusters(
            DynamicPscSourceSplit split, Map<String, Set<String>> metadata) {
        return metadata.containsKey(split.getClusterId())
                && metadata.get(split.getClusterId())
                        .contains(split.getPscTopicUriPartitionSplit().getTopic());
    }

    @Override
    public List<DynamicPscSourceSplit> snapshotState(long checkpointId) {
        List<DynamicPscSourceSplit> splits = snapshotStateFromAllReaders(checkpointId);

        // pending splits should be typically empty, since we do not add splits to pending splits if
        // reader has started
        splits.addAll(pendingSplits);
        return splits;
    }

    private List<DynamicPscSourceSplit> snapshotStateFromAllReaders(long checkpointId) {
        List<DynamicPscSourceSplit> splits = new ArrayList<>();
        for (Map.Entry<String, PscSourceReader<T>> clusterReader : clusterReaderMap.entrySet()) {
            clusterReader
                    .getValue()
                    .snapshotState(checkpointId)
                    .forEach(
                            pscTopicUriPartitionSplit ->
                                    splits.add(
                                            new DynamicPscSourceSplit(
                                                    clusterReader.getKey(), pscTopicUriPartitionSplit)));
        }

        return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        availabilityHelper.resetToUnAvailable();
        syncAvailabilityHelperWithReaders();
        return (CompletableFuture<Void>) availabilityHelper.getAvailableFuture();
    }

    @Override
    public void notifyNoMoreSplits() {
        logger.info("notify no more splits for reader {}", readerContext.getIndexOfSubtask());
        if (pendingSplits.isEmpty()) {
            clusterReaderMap.values().forEach(PscSourceReader::notifyNoMoreSplits);
        }

        isNoMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        logger.debug("Notify checkpoint complete for {}", clusterReaderMap.keySet());
        for (PscSourceReader<T> subReader : clusterReaderMap.values()) {
            subReader.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void close() throws Exception {
        for (PscSourceReader<T> subReader : clusterReaderMap.values()) {
            subReader.close();
        }
        pscClusterMetricGroupManager.close();
    }

    private PscSourceReader<T> createReader(String clusterId) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>>>
                elementsQueue = new FutureCompletingBlockingQueue<>();
        Properties readerSpecificProperties = new Properties();
        PscPropertiesUtil.copyProperties(properties, readerSpecificProperties);
        PscPropertiesUtil.copyProperties(
                Preconditions.checkNotNull(
                        clustersProperties.get(clusterId),
                        "Properties for cluster %s is not found. Current Kafka cluster ids: %s",
                        clusterId,
                        clustersProperties.keySet()),
                readerSpecificProperties);
        PscPropertiesUtil.setClientIdPrefix(readerSpecificProperties, clusterId);

        // layer a kafka cluster group to distinguish metrics by cluster
        PscClusterMetricGroup pscClusterMetricGroup =
                new PscClusterMetricGroup(
                        dynamicPscSourceMetricGroup, readerContext.metricGroup(), clusterId);
        pscClusterMetricGroupManager.register(clusterId, pscClusterMetricGroup);
        PscSourceReaderMetrics pscSourceReaderMetrics =
                new PscSourceReaderMetrics(pscClusterMetricGroup);

        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        // adding pscClusterMetricGroup instead of the sourceReaderMetricGroup
                        // since there could be metric collision, so `kafkaCluster` group is
                        // necessary to
                        // distinguish between instances of this metric
                        return pscClusterMetricGroup.addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });

        PscRecordEmitter<T> recordEmitter = new PscRecordEmitter<>(deserializationSchema);
        return new PscSourceReader<>(
                elementsQueue,
                new PscSourceFetcherManager(
                        elementsQueue,
                        () ->
                        {
                            try {
                                return new PscTopicUriPartitionSplitReaderWrapper(
                                        readerSpecificProperties,
                                        readerContext,
                                        pscSourceReaderMetrics,
                                        clusterId);
                            } catch (ConfigurationException | ClientException e) {
                                throw new RuntimeException(e);
                            }
                        },
                        (ignore) -> {}),
                recordEmitter,
                toConfiguration(readerSpecificProperties),
                readerContext,
                pscSourceReaderMetrics);
    }

    /**
     * In metadata change, we need to reset the availability helper since the number of Kafka source
     * readers could have changed.
     */
    private void completeAndResetAvailabilityHelper() {
        CompletableFuture<?> cachedPreviousFuture = availabilityHelper.getAvailableFuture();
        availabilityHelper = new MultipleFuturesAvailabilityHelper(clusterReaderMap.size());
        syncAvailabilityHelperWithReaders();

        // We cannot immediately complete the previous future here. We must complete it only when
        // the new readers have finished handling the split assignment. Completing the future too
        // early can cause WakeupException (implicitly woken up by invocation to pollNext()) if the
        // reader has not finished resolving the positions of the splits, as seen in flaky unit test
        // errors. There is no error handling for WakeupException in SplitReader's
        // handleSplitChanges.
        availabilityHelper
                .getAvailableFuture()
                .whenComplete(
                        (ignore, t) -> {
                            restartingReaders.set(false);
                            cachedPreviousFuture.complete(null);
                        });
    }

    private void syncAvailabilityHelperWithReaders() {
        int i = 0;
        for (String kafkaClusterId : clusterReaderMap.navigableKeySet()) {
            availabilityHelper.anyOf(i, clusterReaderMap.get(kafkaClusterId).isAvailable());
            i++;
        }
    }

    private void closeAllReadersAndClearState() {
        for (Map.Entry<String, PscSourceReader<T>> entry : clusterReaderMap.entrySet()) {
            try {
                logger.info(
                        "Closing sub reader in reader {} for cluster: {}",
                        readerContext.getIndexOfSubtask(),
                        entry.getKey());
                entry.getValue().close();
                pscClusterMetricGroupManager.close(entry.getKey());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        clusterReaderMap.clear();
        clustersProperties.clear();
    }

    static Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }

    private InputStatus logAndReturnInputStatus(InputStatus inputStatus) {
        if (InputStatus.END_OF_INPUT.equals(inputStatus)) {
            logger.info(
                    "inputStatus={}, subtaskIndex={}",
                    inputStatus,
                    readerContext.getIndexOfSubtask());
        } else {
            logger.trace(
                    "inputStatus={}, subtaskIndex={}",
                    inputStatus,
                    readerContext.getIndexOfSubtask());
        }

        return inputStatus;
    }

    @VisibleForTesting
    public MultipleFuturesAvailabilityHelper getAvailabilityHelper() {
        return availabilityHelper;
    }

    @VisibleForTesting
    public boolean isActivelyConsumingSplits() {
        return isActivelyConsumingSplits;
    }
}
