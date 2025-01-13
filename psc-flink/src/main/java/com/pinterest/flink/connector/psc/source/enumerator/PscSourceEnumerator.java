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

package com.pinterest.flink.connector.psc.source.enumerator;

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriber;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** The enumerator class for PSC source. */
@Internal
public class PscSourceEnumerator
        implements SplitEnumerator<PscTopicUriPartitionSplit, PscSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(PscSourceEnumerator.class);
    private final PscSubscriber subscriber;
    private final OffsetsInitializer startingOffsetInitializer;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final OffsetsInitializer newDiscoveryOffsetsInitializer;
    private final Properties properties;
    private final long partitionDiscoveryIntervalMs;
    private final SplitEnumeratorContext<PscTopicUriPartitionSplit> context;
    private final Boundedness boundedness;

    /** Partitions that have been assigned to readers. */
    private final Set<TopicUriPartition> assignedPartitions;
    /**
     * The partitions that have been discovered during initialization but not assigned to readers
     * yet.
     */
    private final Set<TopicUriPartition> unassignedInitialPartitions;

    /**
     * The discovered and initialized partition splits that are waiting for owner reader to be
     * ready.
     */
    private final Map<Integer, Set<PscTopicUriPartitionSplit>> pendingPartitionSplitAssignment;

    /**
     * The clusterUri for this source, used for PscMetadataClient operations
     */
    private final TopicUri clusterUri;

    /** The consumer group id used for this PscSource. */
    private final String consumerGroupId;

    // Lazily instantiated or mutable fields.
    private PscMetadataClient metadataClient;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // initializing partition discovery has finished.
    private boolean noMoreNewPartitionSplits = false;
    // this flag will be marked as true if initial partitions are discovered after enumerator starts
    private boolean initialDiscoveryFinished;

    public PscSourceEnumerator(
            PscSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<PscTopicUriPartitionSplit> context,
            Boundedness boundedness) {
        this(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness,
                new PscSourceEnumState(Collections.emptySet(), Collections.emptySet(), false));
    }

    public PscSourceEnumerator(
            PscSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<PscTopicUriPartitionSplit> context,
            Boundedness boundedness,
            PscSourceEnumState pscSourceEnumState) {
        this.subscriber = subscriber;
        this.startingOffsetInitializer = startingOffsetInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.newDiscoveryOffsetsInitializer = OffsetsInitializer.earliest();
        this.properties = properties;
        this.context = context;
        this.boundedness = boundedness;

        this.assignedPartitions = new HashSet<>(pscSourceEnumState.assignedPartitions());
        this.pendingPartitionSplitAssignment = new HashMap<>();
        this.partitionDiscoveryIntervalMs =
                PscSourceOptions.getOption(
                        properties,
                        PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);
        this.consumerGroupId = properties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        this.unassignedInitialPartitions =
                new HashSet<>(pscSourceEnumState.unassignedInitialPartitions());
        this.initialDiscoveryFinished = pscSourceEnumState.initialDiscoveryFinished();
        try {
            this.clusterUri = PscFlinkConfiguration.validateAndGetBaseClusterUri(properties);
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException("Failed to get and validate clusterUri from properties", e);
        }
    }

    /**
     * Start the enumerator.
     *
     * <p>Depending on {@link #partitionDiscoveryIntervalMs}, the enumerator will trigger a one-time
     * partition discovery, or schedule a callable for discover partitions periodically.
     *
     * <p>The invoking chain of partition discovery would be:
     *
     * <ol>
     *   <li>{@link #getSubscribedTopicUriPartitions} in worker thread
     *   <li>{@link #checkPartitionChanges} in coordinator thread
     *   <li>{@link #initializePartitionSplits} in worker thread
     *   <li>{@link #handlePartitionSplitChanges} in coordinator thread
     * </ol>
     */
    @Override
    public void start() {
        try {
            metadataClient = getPscMetadataClient();
        } catch (ConfigurationException e) {
            throw new RuntimeException("Failed to get PscMetadataClient", e);
        }
        if (partitionDiscoveryIntervalMs > 0) {
            LOG.info(
                    "Starting the PscSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    consumerGroupId,
                    partitionDiscoveryIntervalMs);
            context.callAsync(
                    this::getSubscribedTopicUriPartitions,
                    this::checkPartitionChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            LOG.info(
                    "Starting the PscSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    consumerGroupId);
            context.callAsync(this::getSubscribedTopicUriPartitions, this::checkPartitionChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the psc source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void addSplitsBack(List<PscTopicUriPartitionSplit> splits, int subtaskId) {
        addPartitionSplitChangeToPendingAssignments(splits);

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingPartitionSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to PscSourceEnumerator for consumer group {}.",
                subtaskId,
                consumerGroupId);
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }

    @Override
    public PscSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new PscSourceEnumState(assignedPartitions, unassignedInitialPartitions, initialDiscoveryFinished);
    }

    @Override
    public void close() throws IOException {
        if (metadataClient != null) {
            metadataClient.close();
        }
    }

    // ----------------- private methods -------------------

    /**
     * List subscribed topic partitions on PubSub brokers.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * requires network I/O with PubSub brokers.
     *
     * @return Set of subscribed {@link TopicUriPartition}s
     */
    private Set<TopicUriPartition> getSubscribedTopicUriPartitions() {
        return subscriber.getSubscribedTopicUriPartitions(metadataClient, clusterUri);
    }

    /**
     * Check if there's any partition changes within subscribed topic partitions fetched by worker
     * thread, and invoke {@link PscSourceEnumerator#initializePartitionSplits(PartitionChange)}
     * in worker thread to initialize splits for new partitions.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param fetchedPartitions Map from topic name to its description
     * @param t Exception in worker thread
     */
    private void checkPartitionChanges(Set<TopicUriPartition> fetchedPartitions, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException(
                    "Failed to list subscribed topic partitions due to ", t);
        }
        if (!initialDiscoveryFinished) {
            unassignedInitialPartitions.addAll(fetchedPartitions);
            initialDiscoveryFinished = true;
        }
        final PartitionChange partitionChange = getPartitionChange(fetchedPartitions);
        if (partitionChange.isEmpty()) {
            return;
        }
        context.callAsync(
                () -> initializePartitionSplits(partitionChange),
                this::handlePartitionSplitChanges);
    }

    /**
     * Initialize splits for newly discovered partitions.
     *
     * <p>Enumerator will be responsible for fetching offsets when initializing splits if:
     *
     * <ul>
     *   <li>using timestamp for initializing offset
     *   <li>or using specified offset, but the offset is not provided for the newly discovered
     *       partitions
     * </ul>
     *
     * <p>Otherwise offsets will be initialized by readers.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * potentially requires network I/O with PubSub brokers for fetching offsets.
     *
     * @param partitionChange Newly discovered and removed partitions
     * @return {@link PscTopicUriPartitionSplit} of new partitions and {@link TopicUriPartition} of removed
     *     partitions
     */
    private PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
        Set<TopicUriPartition> newPartitions =
                Collections.unmodifiableSet(partitionChange.getNewPartitions());

        OffsetsInitializer.PartitionOffsetsRetriever offsetsRetriever = getOffsetsRetriever();

        // initial partitions use OffsetsInitializer specified by the user while new partitions use
        // EARLIEST
        Map<TopicUriPartition, Long> startingOffsets = new HashMap<>();
        startingOffsets.putAll(
                newDiscoveryOffsetsInitializer.getPartitionOffsets(
                        newPartitions, offsetsRetriever));
        startingOffsets.putAll(
                startingOffsetInitializer.getPartitionOffsets(
                        unassignedInitialPartitions, offsetsRetriever));
        Map<TopicUriPartition, Long> stoppingOffsets =
                stoppingOffsetInitializer.getPartitionOffsets(newPartitions, offsetsRetriever);

        Set<PscTopicUriPartitionSplit> partitionSplits = new HashSet<>(newPartitions.size());
        for (TopicUriPartition tp : newPartitions) {
            Long startingOffset = startingOffsets.get(tp);
            long stoppingOffset =
                    stoppingOffsets.getOrDefault(tp, PscTopicUriPartitionSplit.NO_STOPPING_OFFSET);
            partitionSplits.add(new PscTopicUriPartitionSplit(tp, startingOffset, stoppingOffset));
        }
        return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
    }

    /**
     * Mark partition splits initialized by {@link
     * PscSourceEnumerator#initializePartitionSplits(PartitionChange)} as pending and try to
     * assign pending splits to registered readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param partitionSplitChange Partition split changes
     * @param t Exception in worker thread
     */
    private void handlePartitionSplitChanges(
            PartitionSplitChange partitionSplitChange, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to initialize partition splits due to ", t);
        }
        if (partitionDiscoveryIntervalMs <= 0) {
            LOG.debug("Partition discovery is disabled.");
            noMoreNewPartitionSplits = true;
        }
        // TODO: Handle removed partitions.
        addPartitionSplitChangeToPendingAssignments(partitionSplitChange.newPartitionSplits);
        assignPendingPartitionSplits(context.registeredReaders().keySet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void addPartitionSplitChangeToPendingAssignments(
            Collection<PscTopicUriPartitionSplit> newPartitionSplits) {
        int numReaders = context.currentParallelism();
        for (PscTopicUriPartitionSplit split : newPartitionSplits) {
            int ownerReader = getSplitOwner(split.getTopicUriPartition(), numReaders);
            pendingPartitionSplitAssignment
                    .computeIfAbsent(ownerReader, r -> new HashSet<>())
                    .add(split);
            LOG.info("Adding split {} to reader {}", split, ownerReader);
            Thread.dumpStack();
        }
        LOG.debug(
                "Assigned {} to {} readers of consumer group {}.",
                newPartitionSplits,
                numReaders,
                consumerGroupId);
    }

    // This method should only be invoked in the coordinator executor thread.
    private void assignPendingPartitionSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<PscTopicUriPartitionSplit>> incrementalAssignment = new HashMap<>();

        // Check if there's any pending splits for given readers
        for (int pendingReader : pendingReaders) {
            checkReaderRegistered(pendingReader);

            // Remove pending assignment for the reader
            final Set<PscTopicUriPartitionSplit> pendingAssignmentForReader =
                    pendingPartitionSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Put pending assignment into incremental assignment
                incrementalAssignment
                        .computeIfAbsent(pendingReader, (ignored) -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);

                // Mark pending partitions as already assigned
                pendingAssignmentForReader.forEach(
                        split -> {
                            assignedPartitions.add(split.getTopicUriPartition());
                            unassignedInitialPartitions.remove(split.getTopicUriPartition());
                        });
            }
        }

        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("Assigning splits to readers {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }

        // If periodically partition discovery is disabled and the initializing discovery has done,
        // signal NoMoreSplitsEvent to pending readers
        if (noMoreNewPartitionSplits && boundedness == Boundedness.BOUNDED) {
            LOG.debug(
                    "No more PscTopicUriPartitionSplits to assign. Sending NoMoreSplitsEvent to reader {}"
                            + " in consumer group {}.",
                    pendingReaders,
                    consumerGroupId);
            pendingReaders.forEach(context::signalNoMoreSplits);
        }
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @VisibleForTesting
    PartitionChange getPartitionChange(Set<TopicUriPartition> fetchedPartitions) {
        final Set<TopicUriPartition> removedPartitions = new HashSet<>();
        Consumer<TopicUriPartition> dedupOrMarkAsRemoved =
                (tp) -> {
                    if (!fetchedPartitions.remove(tp)) {
                        removedPartitions.add(tp);
                    }
                };
        LOG.info("Assigned partitions: {}", assignedPartitions);
        assignedPartitions.forEach(dedupOrMarkAsRemoved);
        pendingPartitionSplitAssignment.forEach(
                (reader, splits) ->
                        splits.forEach(
                                split -> dedupOrMarkAsRemoved.accept(split.getTopicUriPartition())));

        if (!fetchedPartitions.isEmpty()) {
            LOG.info("Discovered new partitions: {}", fetchedPartitions);
        }
        if (!removedPartitions.isEmpty()) {
            LOG.info("Discovered removed partitions: {}", removedPartitions);
        }

        return new PartitionChange(fetchedPartitions, removedPartitions);
    }

    private PscMetadataClient getPscMetadataClient() throws ConfigurationException {
        Properties metadataClientProps = new Properties();
        deepCopyProperties(properties, metadataClientProps);
        // set client id prefix
        String clientIdPrefix =
                metadataClientProps.getProperty(PscSourceOptions.CLIENT_ID_PREFIX.key());
        metadataClientProps.setProperty(
                PscConfiguration.PSC_METADATA_CLIENT_ID, clientIdPrefix + "-enumerator-admin-client");
        return new PscMetadataClient(PscConfigurationUtils.propertiesToPscConfiguration(metadataClientProps));
    }

    private OffsetsInitializer.PartitionOffsetsRetriever getOffsetsRetriever() {
        String groupId = properties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        return new PartitionOffsetsRetrieverImpl(metadataClient, clusterUri, groupId);
    }

    /**
     * Returns the index of the target subtask that a specific TopicUriPartition should be assigned
     * to.
     *
     * <p>The resulting distribution of partitions of a single topic has the following contract:
     *
     * <ul>
     *   <li>1. Uniformly distributed across subtasks
     *   <li>2. Partitions are round-robin distributed (strictly clockwise w.r.t. ascending subtask
     *       indices) by using the partition id as the offset from a starting index (i.e., the index
     *       of the subtask which partition 0 of the topic will be assigned to, determined using the
     *       topic name).
     * </ul>
     *
     * @param tp the PSC TopicUriPartition to assign.
     * @param numReaders the total number of readers.
     * @return the id of the subtask that owns the split.
     */
    @VisibleForTesting
    static int getSplitOwner(TopicUriPartition tp, int numReaders) {
        int startIndex = ((tp.getTopicUriAsString().hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // here, the assumption is that the id of partitions are always ascending
        // starting from 0, and therefore can be used directly as the offset clockwise from the
        // start index
        return (startIndex + tp.getPartition()) % numReaders;
    }

    @VisibleForTesting
    static void deepCopyProperties(Properties from, Properties to) {
        for (String key : from.stringPropertyNames()) {
            to.setProperty(key, from.getProperty(key));
        }
    }

    // --------------- private class ---------------

    /** A container class to hold the newly added partitions and removed partitions. */
    @VisibleForTesting
    static class PartitionChange {
        private final Set<TopicUriPartition> newPartitions;
        private final Set<TopicUriPartition> removedPartitions;

        PartitionChange(Set<TopicUriPartition> newPartitions, Set<TopicUriPartition> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<TopicUriPartition> getNewPartitions() {
            return newPartitions;
        }

        public Set<TopicUriPartition> getRemovedPartitions() {
            return removedPartitions;
        }

        public boolean isEmpty() {
            return newPartitions.isEmpty() && removedPartitions.isEmpty();
        }
    }

    private static class PartitionSplitChange {
        private final Set<PscTopicUriPartitionSplit> newPartitionSplits;
        private final Set<TopicUriPartition> removedPartitions;

        private PartitionSplitChange(
                Set<PscTopicUriPartitionSplit> newPartitionSplits,
                Set<TopicUriPartition> removedPartitions) {
            this.newPartitionSplits = Collections.unmodifiableSet(newPartitionSplits);
            this.removedPartitions = Collections.unmodifiableSet(removedPartitions);
        }
    }

    /** The implementation for offsets retriever with a consumer and an admin client. */
    @VisibleForTesting
    public static class PartitionOffsetsRetrieverImpl
            implements OffsetsInitializer.PartitionOffsetsRetriever, AutoCloseable {
        private final PscMetadataClient metadataClient;
        private final String groupId;
        private final TopicUri clusterUri;

        public PartitionOffsetsRetrieverImpl(PscMetadataClient metadataClient, TopicUri clusterUri, String groupId) {
            this.metadataClient = metadataClient;
            this.clusterUri = clusterUri;
            this.groupId = groupId;
        }

        @Override
        public Map<TopicUriPartition, Long> committedOffsets(Collection<TopicUriPartition> partitions) {
            try {
                return metadataClient
                        .listOffsetsForConsumerGroup(clusterUri, groupId, partitions, Duration.ofMillis(Long.MAX_VALUE));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for consumer group " + groupId, e);
            } catch (ExecutionException | TimeoutException e) {
                throw new FlinkRuntimeException(
                        "Failed to fetch committed offsets for consumer group "
                                + groupId
                                + " due to",
                        e);
            } catch (TopicUriSyntaxException e) {
                throw new FlinkRuntimeException(e);
            }
        }

        /**
         * List offsets for the specified partitions and OffsetSpec. This operation enables to find
         * the beginning offset, end offset as well as the offset matching a timestamp in
         * partitions.
         *
         * @see PscMetadataClient#listOffsets(TopicUri, Map, Duration)
         * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
         * @return The list offsets result.
         */
        private Map<TopicUriPartition, Long> listOffsets(
                Map<TopicUriPartition, PscMetadataClient.MetadataClientOption> topicPartitionOffsets) {
            try {
                return metadataClient
                        .listOffsets(clusterUri, topicPartitionOffsets, Duration.ofMillis(Long.MAX_VALUE));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for topic partitions: "
                                + topicPartitionOffsets,
                        e);
            } catch (ExecutionException | TimeoutException e) {
                throw new FlinkRuntimeException(
                        "Failed to list offsets for topic partitions: "
                                + topicPartitionOffsets
                                + " due to",
                        e);
            } catch (TopicUriSyntaxException e) {
                throw new FlinkRuntimeException(e);
            }
        }

        private Map<TopicUriPartition, Long> listOffsetsForTimestamps(
                Map<TopicUriPartition, Long> topicUriPartitionsAndTimestamps) {
            try {
                return metadataClient
                        .listOffsetsForTimestamps(clusterUri, topicUriPartitionsAndTimestamps, Duration.ofMillis(Long.MAX_VALUE));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for topic partitions: "
                                + topicUriPartitionsAndTimestamps,
                        e);
            } catch (ExecutionException | TimeoutException e) {
                throw new FlinkRuntimeException(
                        "Failed to list offsets for topic partitions: "
                                + topicUriPartitionsAndTimestamps
                                + " due to",
                        e);
            } catch (TopicUriSyntaxException e) {
                throw new FlinkRuntimeException(e);
            }
        }

        private Map<TopicUriPartition, Long> listOffsets(
                Collection<TopicUriPartition> partitions, PscMetadataClient.MetadataClientOption offsetSpec) {
            return listOffsets(
                            partitions.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    partition -> partition, __ -> offsetSpec)));
        }

        @Override
        public Map<TopicUriPartition, Long> endOffsets(Collection<TopicUriPartition> partitions) {
            return listOffsets(partitions, PscMetadataClient.MetadataClientOption.OFFSET_SPEC_LATEST);
        }

        @Override
        public Map<TopicUriPartition, Long> beginningOffsets(Collection<TopicUriPartition> partitions) {
            return listOffsets(partitions, PscMetadataClient.MetadataClientOption.OFFSET_SPEC_EARLIEST);
        }

        @Override
        public Map<TopicUriPartition, Long> offsetsForTimes(
                Map<TopicUriPartition, Long> timestampsToSearch) {
            return listOffsetsForTimestamps(timestampsToSearch).entrySet().stream()
                    .filter(entry -> entry.getValue() >= 0)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public void close() throws Exception {
            metadataClient.close();
        }
    }
}
