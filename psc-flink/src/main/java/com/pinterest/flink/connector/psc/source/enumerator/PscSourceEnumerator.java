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

import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriber;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metadata.client.PscMetadataClient;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** The enumerator class for Kafka source. */
@Internal
public class PscSourceEnumerator
        implements SplitEnumerator<PscTopicUriPartitionSplit, PscSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(PscSourceEnumerator.class);
    private final PscSubscriber subscriber;
    private final OffsetsInitializer startingOffsetInitializer;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final Properties properties;
    private final long partitionDiscoveryIntervalMs;
    private final SplitEnumeratorContext<PscTopicUriPartitionSplit> context;
    private final Boundedness boundedness;

    /** Partitions that have been assigned to readers. */
    private final Set<TopicUriPartition> assignedPartitions;

    /**
     * The discovered and initialized partition splits that are waiting for owner reader to be
     * ready.
     */
    private final Map<Integer, Set<PscTopicUriPartitionSplit>> pendingPartitionSplitAssignment;

    /** The consumer group id used for this KafkaSource. */
    private final String consumerGroupId;

    // Lazily instantiated or mutable fields.
    private PscMetadataClient metadataClient;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // initializing partition discovery has finished.
    private boolean noMoreNewPartitionSplits = false;

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
                Collections.emptySet());
    }

    public PscSourceEnumerator(
            PscSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<PscTopicUriPartitionSplit> context,
            Boundedness boundedness,
            Set<TopicUriPartition> assignedPartitions) {
        this.subscriber = subscriber;
        this.startingOffsetInitializer = startingOffsetInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.properties = properties;
        this.context = context;
        this.boundedness = boundedness;

        this.assignedPartitions = new HashSet<>(assignedPartitions);
        this.pendingPartitionSplitAssignment = new HashMap<>();
        this.partitionDiscoveryIntervalMs =
                PscSourceOptions.getOption(
                        properties,
                        PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);
        this.consumerGroupId = properties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
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
        metadataClient = getPscMetadataClient();
        if (partitionDiscoveryIntervalMs > 0) {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
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
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    consumerGroupId);
            context.callAsync(this::getSubscribedTopicUriPartitions, this::checkPartitionChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the kafka source pushes splits eagerly, rather than act upon split requests
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
                "Adding reader {} to KafkaSourceEnumerator for consumer group {}.",
                subtaskId,
                consumerGroupId);
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }

    @Override
    public PscSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new PscSourceEnumState(assignedPartitions);
    }

    @Override
    public void close() throws IOException {
        if (metadataClient != null) {
            metadataClient.close();
        }
    }

    // ----------------- private methods -------------------

    /**
     * List subscribed topic partitions on Kafka brokers.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * requires network I/O with Kafka brokers.
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
     * potentially requires network I/O with Kafka brokers for fetching offsets.
     *
     * @param partitionChange Newly discovered and removed partitions
     * @return {@link PscTopicUriPartitionSplit} of new partitions and {@link TopicUriPartition} of removed
     *     partitions
     */
    private PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
        Set<TopicUriPartition> newPartitions =
                Collections.unmodifiableSet(partitionChange.getNewPartitions());
        OffsetsInitializer.PartitionOffsetsRetriever offsetsRetriever = getOffsetsRetriever();

        Map<TopicUriPartition, Long> startingOffsets =
                startingOffsetInitializer.getPartitionOffsets(newPartitions, offsetsRetriever);
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
        if (partitionDiscoveryIntervalMs < 0) {
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
                        split -> assignedPartitions.add(split.getTopicUriPartition()));
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
                    "No more KafkaPartitionSplits to assign. Sending NoMoreSplitsEvent to reader {}"
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
        return new PartitionOffsetsRetrieverImpl(metadataClient, groupId);
    }

    /**
     * Returns the index of the target subtask that a specific Kafka partition should be assigned
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
     * @param tp the Kafka partition to assign.
     * @param numReaders the total number of readers.
     * @return the id of the subtask that owns the split.
     */
    @VisibleForTesting
    static int getSplitOwner(TopicUriPartition tp, int numReaders) {
        int startIndex = ((tp.getTopicUriAsString().hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // here, the assumption is that the id of Kafka partitions are always ascending
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

        public PartitionOffsetsRetrieverImpl(PscMetadataClient adminClient, String groupId) {
            this.metadataClient = adminClient;
            this.groupId = groupId;
        }

        @Override
        public Map<TopicUriPartition, Long> committedOffsets(Collection<TopicUriPartition> partitions) {
//            ListConsumerGroupOffsetsOptions options =
//                    new ListConsumerGroupOffsetsOptions()
//                            .topicPartitions(new ArrayList<>(partitions));
            try {
                return metadataClient
                        .listOffsetsForConsumerGroup(clusterUri, groupId, partitions, Duration.ofMillis(Long.MAX_VALUE));

//
//                return metadataClient
//                        .listConsumerGroupOffsets(groupId, options)
//                        .partitionsToOffsetAndMetadata()
//                        .thenApply(
//                                result -> {
//                                    Map<TopicPartition, Long> offsets = new HashMap<>();
//                                    result.forEach(
//                                            (tp, oam) -> {
//                                                if (oam != null) {
//                                                    offsets.put(tp, oam.offset());
//                                                }
//                                            });
//                                    return offsets;
//                                })
//                        .get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for consumer group " + groupId, e);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "Failed to fetch committed offsets for consumer group "
                                + groupId
                                + " due to",
                        e);
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
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "Failed to list offsets for topic partitions: "
                                + topicPartitionOffsets
                                + " due to",
                        e);
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
            return listOffsets(
                            timestampsToSearch.entrySet().stream()
                                    .collect(
                                            Collectors.toMap(
                                                    Map.Entry::getKey,
                                                    entry ->
                                                            OffsetSpec.forTimestamp(
                                                                    entry.getValue()))))
                    .entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry ->
                                            new OffsetAndTimestamp(
                                                    entry.getValue().offset(),
                                                    entry.getValue().timestamp(),
                                                    entry.getValue().leaderEpoch())));
        }

        @Override
        public void close() throws Exception {
            metadataClient.close();
        }
    }
}
