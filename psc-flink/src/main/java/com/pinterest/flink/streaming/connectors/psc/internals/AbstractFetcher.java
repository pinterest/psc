/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc.internals;

import java.util.Collection;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import com.pinterest.flink.streaming.connectors.psc.config.OffsetCommitMode;
import com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.COMMITTED_OFFSETS_METRICS_GAUGE;
import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.CURRENT_OFFSETS_METRICS_GAUGE;
import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.LEGACY_COMMITTED_OFFSETS_METRICS_GROUP;
import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.LEGACY_CURRENT_OFFSETS_METRICS_GROUP;
import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.OFFSETS_BY_PARTITION_METRICS_GROUP;
import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.OFFSETS_BY_TOPIC_METRICS_GROUP;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all fetchers, which implement the connections to and pill records from backend pubsub.
 *
 * <p>This fetcher base class implements the logic around emitting records and tracking offsets,
 * as well as around the optional timestamp assignment and watermark generation.
 *
 * @param <T>    The type of elements deserialized from PSC's byte records, and emitted into
 *               the Flink data streams.
 * @param <TUPH> The type of topic URI/partition identifier used by PSC in the specific version.
 */
@Internal
public abstract class AbstractFetcher<T, TUPH> {

    private static final int NO_TIMESTAMPS_WATERMARKS = 0;
    private static final int WITH_WATERMARK_GENERATOR = 1;

    // ------------------------------------------------------------------------

    /**
     * The source context to emit records and watermarks to.
     */
    protected final SourceContext<T> sourceContext;

    /**
     * Wrapper around our SourceContext for allowing the {@link org.apache.flink.api.common.eventtime.WatermarkGenerator}
     * to emit watermarks and mark idleness.
     */
    protected final WatermarkOutput watermarkOutput;

    /**
     * {@link WatermarkOutputMultiplexer} for supporting per-partition watermark generation.
     */
    private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;

    /**
     * The lock that guarantees that record emission and state updates are atomic,
     * from the view of taking a checkpoint.
     */
    protected final Object checkpointLock;

    /**
     * All partitions (and their state) that this fetcher is subscribed to.
     */
    private final Map<PscTopicUriPartition, PscTopicUriPartitionState<T, TUPH>> subscribedTopicUriPartitionStates;

    /**
     * Queue of partitions that are not yet assigned to any PSC clients for consuming.
     * PSC version-specific implementations of {@link AbstractFetcher#runFetchLoop()}
     * should continuously poll this queue for unassigned partitions, and start consuming
     * them accordingly.
     *
     * <p>All partitions added to this queue are guaranteed to have been added
     * to {@link #subscribedTopicUriPartitionStates} already.
     */
    protected final ClosableBlockingQueue<PscTopicUriPartitionState<T, TUPH>> unassignedTopicUriPartitionsQueue;

    /**
     * The mode describing whether the fetcher also generates timestamps and watermarks.
     */
    private final int timestampWatermarkMode;

    /**
     * Optional watermark strategy that will be run per PSC topic URI partition, to exploit per-partition
     * timestamp characteristics. The watermark strategy is kept in serialized form, to deserialize
     * it into multiple copies.
     */
    private final SerializedValue<WatermarkStrategy<T>> watermarkStrategy;

    /**
     * User class loader used to deserialize watermark assigners.
     */
    private final ClassLoader userCodeClassLoader;

    // ------------------------------------------------------------------------
    //  Metrics
    // ------------------------------------------------------------------------

    /**
     * Flag indicating whether or not metrics should be exposed.
     * If {@code true}, offset metrics (e.g. current offset, committed offset) and
     * PSC-shipped metrics will be registered.
     */
    private final boolean useMetrics;

    /**
     * The metric group which all metrics for the consumer should be registered to.
     * This metric group is defined under the user scope {@link PscConsumerMetricConstants#PSC_CONSUMER_METRICS_GROUP}.
     */
    private final MetricGroup consumerMetricGroup;

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    private final MetricGroup legacyCurrentOffsetsMetricGroup;

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    private final MetricGroup legacyCommittedOffsetsMetricGroup;

    protected AbstractFetcher(
            SourceContext<T> sourceContext,
            Map<PscTopicUriPartition, Long> seedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {
        this.sourceContext = checkNotNull(sourceContext);
        this.watermarkOutput = new SourceContextWatermarkOutputAdapter<>(sourceContext);
        this.watermarkOutputMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.userCodeClassLoader = checkNotNull(userCodeClassLoader);

        this.useMetrics = useMetrics;
        this.consumerMetricGroup = checkNotNull(consumerMetricGroup);
        this.legacyCurrentOffsetsMetricGroup = consumerMetricGroup.addGroup(LEGACY_CURRENT_OFFSETS_METRICS_GROUP);
        this.legacyCommittedOffsetsMetricGroup = consumerMetricGroup.addGroup(LEGACY_COMMITTED_OFFSETS_METRICS_GROUP);

        this.watermarkStrategy = watermarkStrategy;

        if (watermarkStrategy == null) {
            timestampWatermarkMode = NO_TIMESTAMPS_WATERMARKS;
        } else {
            timestampWatermarkMode = WITH_WATERMARK_GENERATOR;
        }

        this.unassignedTopicUriPartitionsQueue = new ClosableBlockingQueue<>();

        // initialize subscribed partition states with seed partitions
        this.subscribedTopicUriPartitionStates = createPartitionStateHolders(
                seedPartitionsWithInitialOffsets,
                timestampWatermarkMode,
                watermarkStrategy,
                userCodeClassLoader);

        // check that all seed partition states have a defined offset
        for (PscTopicUriPartitionState<?, ?> partitionState : subscribedTopicUriPartitionStates.values()) {
            if (!partitionState.isOffsetDefined()) {
                throw new IllegalArgumentException("The fetcher was assigned seed partitions with undefined initial offsets.");
            }
        }

        // all seed partitions are not assigned yet, so should be added to the unassigned partitions queue
        for (PscTopicUriPartitionState<T, TUPH> partition : subscribedTopicUriPartitionStates.values()) {
            unassignedTopicUriPartitionsQueue.add(partition);
        }

        // register metrics for the initial seed partitions
        if (useMetrics) {
            registerOffsetMetrics(consumerMetricGroup, subscribedTopicUriPartitionStates.values());
        }

        // if we have periodic watermarks, kick off the interval scheduler
        if (timestampWatermarkMode == WITH_WATERMARK_GENERATOR && autoWatermarkInterval > 0) {
            PeriodicWatermarkEmitter<T, TUPH> periodicEmitter = new PeriodicWatermarkEmitter<>(
                    checkpointLock,
                    subscribedTopicUriPartitionStates.values(),
                    watermarkOutputMultiplexer,
                    processingTimeProvider,
                    autoWatermarkInterval);

            periodicEmitter.start();
        }
    }

    /**
     * Adds a list of newly discovered partitions to the fetcher for consuming.
     *
     * <p>This method creates the partition state holder for each new partition, using
     * {@link PscTopicUriPartitionStateSentinel#EARLIEST_OFFSET} as the starting offset.
     * It uses the earliest offset because there may be delay in discovering a partition
     * after it was created and started receiving records.
     *
     * <p>After the state representation for a partition is created, it is added to the
     * unassigned partitions queue to await to be consumed.
     *
     * @param newPartitions discovered partitions to add
     */
    public void addDiscoveredPartitions(List<PscTopicUriPartition> newPartitions) throws IOException, ClassNotFoundException {
        Map<PscTopicUriPartition, PscTopicUriPartitionState<T, TUPH>> newPartitionStates = createPartitionStateHolders(
                newPartitions,
                PscTopicUriPartitionStateSentinel.EARLIEST_OFFSET,
                timestampWatermarkMode,
                watermarkStrategy,
                userCodeClassLoader);

        if (useMetrics) {
            registerOffsetMetrics(consumerMetricGroup, newPartitionStates.values());
        }

        for (Map.Entry<PscTopicUriPartition, PscTopicUriPartitionState<T, TUPH>> newPartitionStateEntry : newPartitionStates.entrySet()) {
            // the ordering is crucial here; first register the state holder, then
            // push it to the partitions queue to be read
            subscribedTopicUriPartitionStates.put(newPartitionStateEntry.getKey(), newPartitionStateEntry.getValue());
            unassignedTopicUriPartitionsQueue.add(newPartitionStateEntry.getValue());
        }
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    /**
     * Gets all partitions (with partition state) that this fetcher is subscribed to.
     *
     * @return All subscribed partitions.
     */
    protected final Map<PscTopicUriPartition, PscTopicUriPartitionState<T, TUPH>> subscribedPartitionStates() {
        return subscribedTopicUriPartitionStates;
    }

    // ------------------------------------------------------------------------
    //  Core fetcher work methods
    // ------------------------------------------------------------------------

    public abstract void runFetchLoop() throws Exception;

    public abstract void cancel();

    // ------------------------------------------------------------------------
    //  PSC version specifics
    // ------------------------------------------------------------------------

    /**
     * Commits the given partition offsets to the backend pubsub
     * This method is only ever called when the offset commit mode of
     * the consumer is {@link OffsetCommitMode#ON_CHECKPOINTS}.
     *
     * <p>The given offsets are the internal checkpointed offsets, representing
     * the last processed record of each partition. Version-specific implementations of this method
     * need to hold the contract that the given offsets must be incremented by 1 before
     * committing them, so that committed offsets represent "the next record to process".
     *
     * @param offsets        The offsets to commit to PSC (implementations must increment offsets by 1 before committing).
     * @param commitCallback The callback that the user should trigger when a commit request completes or fails.
     * @throws Exception This method forwards exceptions.
     */
    public final void commitInternalOffsets(
            Map<PscTopicUriPartition, Long> offsets,
            @Nonnull PscCommitCallback commitCallback) throws Exception {
        // Ignore sentinels. They might appear here if snapshot has started before actual offsets values
        // replaced sentinels
        doCommitInternalOffsets(filterOutSentinels(offsets), commitCallback);
    }

    protected abstract void doCommitInternalOffsets(
            Map<PscTopicUriPartition, Long> offsets,
            @Nonnull PscCommitCallback commitCallback) throws Exception;

    private Map<PscTopicUriPartition, Long> filterOutSentinels(Map<PscTopicUriPartition, Long> offsets) {
        return offsets.entrySet()
                .stream()
                .filter(entry -> !PscTopicUriPartitionStateSentinel.isSentinel(entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Creates the PSC version specific representation of the given
     * topic URI partition.
     *
     * @param partition The Flink representation of the PSC topic URI partition.
     * @return The version-specific PSC representation of the PSC topic URI partition.
     */
    protected abstract TUPH createPscTopicUriPartitionHandle(PscTopicUriPartition partition);

    // ------------------------------------------------------------------------
    //  snapshot and restore the state
    // ------------------------------------------------------------------------

    /**
     * Takes a snapshot of the partition offsets.
     *
     * <p>Important: This method must be called under the checkpoint lock.
     *
     * @return A map from partition to current offset.
     */
    public HashMap<PscTopicUriPartition, Long> snapshotCurrentState() {
        // this method assumes that the checkpoint lock is held
        assert Thread.holdsLock(checkpointLock);

        HashMap<PscTopicUriPartition, Long> state = new HashMap<>(subscribedTopicUriPartitionStates.size());
        for (PscTopicUriPartitionState<T, TUPH> partition : subscribedTopicUriPartitionStates.values()) {
            state.put(partition.getPscTopicUriPartition(), partition.getOffset());
        }
        return state;
    }

    // ------------------------------------------------------------------------
    //  emitting records
    // ------------------------------------------------------------------------

    /**
     * Emits a record attaching a timestamp to it.
     *
     * @param records              The records to emit
     * @param partitionState       The state of the PSC topic URI partition from which the record was fetched
     * @param offset               The offset of the corresponding PSC record
     * @param pubsubEventTimestamp The timestamp of the PSC record
     */
    protected void emitRecordsWithTimestamps(
            Queue<T> records,
            PscTopicUriPartitionState<T, TUPH> partitionState,
            long offset,
            long pubsubEventTimestamp) {
        // emit the records, using the checkpoint lock to guarantee
        // atomicity of record emission and offset state update
        synchronized (checkpointLock) {
            T record;
            while ((record = records.poll()) != null) {
                long timestamp = partitionState.extractTimestamp(record, pubsubEventTimestamp);
                sourceContext.collectWithTimestamp(record, timestamp);

                // this might emit a watermark, so do it after emitting the record
                partitionState.onEvent(record, timestamp);
            }
            partitionState.setOffset(offset);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Utility method that takes the topic partitions and creates the topic partition state
     * holders, depending on the timestamp / watermark mode.
     */
    private Map<PscTopicUriPartition, PscTopicUriPartitionState<T, TUPH>> createPartitionStateHolders(
        Map<PscTopicUriPartition, Long> partitionsToInitialOffsets,
            int timestampWatermarkMode,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

        // CopyOnWrite as adding discovered partitions could happen in parallel
        // while different threads iterate the partitions list
        Map<PscTopicUriPartition, PscTopicUriPartitionState<T, TUPH>> partitionStates = new HashMap<>();

        switch (timestampWatermarkMode) {
            case NO_TIMESTAMPS_WATERMARKS: {
                for (Map.Entry<PscTopicUriPartition, Long> partitionEntry : partitionsToInitialOffsets.entrySet()) {
                    // create the PSC version specific partition handle
                    TUPH pscTopicUriPartitionHandle = createPscTopicUriPartitionHandle(partitionEntry.getKey());

                    PscTopicUriPartitionState<T, TUPH> partitionState =
                            new PscTopicUriPartitionState<>(partitionEntry.getKey(), pscTopicUriPartitionHandle);
                    partitionState.setOffset(partitionEntry.getValue());

                    partitionStates.put(partitionEntry.getKey(), partitionState);;
                }

                return partitionStates;
            }

            case WITH_WATERMARK_GENERATOR: {
                for (Map.Entry<PscTopicUriPartition, Long> partitionEntry : partitionsToInitialOffsets.entrySet()) {
                    final PscTopicUriPartition pscTopicUriPartition = partitionEntry.getKey();
                    TUPH pscTopicUriPartitionHandle = createPscTopicUriPartitionHandle(pscTopicUriPartition);
                    WatermarkStrategy<T> deserializedWatermarkStrategy = watermarkStrategy.deserializeValue(
                            userCodeClassLoader);

                    // the format of the ID does not matter, as long as it is unique
                    final String partitionId = pscTopicUriPartition.getTopicUriStr() + '-' + pscTopicUriPartition.getPartition();
                    watermarkOutputMultiplexer.registerNewOutput(partitionId);
                    WatermarkOutput immediateOutput =
                            watermarkOutputMultiplexer.getImmediateOutput(partitionId);
                    WatermarkOutput deferredOutput =
                            watermarkOutputMultiplexer.getDeferredOutput(partitionId);

                    PscTopicUriPartitionStateWithWatermarkGenerator<T, TUPH> partitionState =
                            new PscTopicUriPartitionStateWithWatermarkGenerator<>(
                                    partitionEntry.getKey(),
                                    pscTopicUriPartitionHandle,
                                    deserializedWatermarkStrategy.createTimestampAssigner(() -> consumerMetricGroup),
                                    deserializedWatermarkStrategy.createWatermarkGenerator(() -> consumerMetricGroup),
                                    immediateOutput,
                                    deferredOutput);

                    partitionState.setOffset(partitionEntry.getValue());

                    partitionStates.put(partitionEntry.getKey(), partitionState);
                }

                return partitionStates;
            }

            default:
                // cannot happen, add this as a guard for the future
                throw new RuntimeException();
        }
    }

    /**
     * Shortcut variant of {@link #createPartitionStateHolders(Map, int, SerializedValue, ClassLoader)}
     * that uses the same offset for all partitions when creating their state holders.
     */
    private Map<PscTopicUriPartition, PscTopicUriPartitionState<T, TUPH>> createPartitionStateHolders(
            List<PscTopicUriPartition> partitions,
            long initialOffset,
            int timestampWatermarkMode,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

        Map<PscTopicUriPartition, Long> partitionsToInitialOffset = new HashMap<>(partitions.size());
        for (PscTopicUriPartition partition : partitions) {
            partitionsToInitialOffset.put(partition, initialOffset);
        }

        return createPartitionStateHolders(
                partitionsToInitialOffset,
                timestampWatermarkMode,
                watermarkStrategy,
                userCodeClassLoader);
    }

    // ------------------------- Metrics ----------------------------------

    /**
     * For each partition, register a new metric group to expose current offsets and committed offsets.
     * Per-partition metric groups can be scoped by user variables {@link PscConsumerMetricConstants#OFFSETS_BY_TOPIC_METRICS_GROUP}
     * and {@link PscConsumerMetricConstants#OFFSETS_BY_PARTITION_METRICS_GROUP}.
     *
     * <p>Note: this method also registers gauges for deprecated offset metrics, to maintain backwards compatibility.
     *
     * @param consumerMetricGroup   The consumer metric group
     * @param partitionOffsetStates The partition offset state holders, whose values will be used to update metrics
     */
    private void registerOffsetMetrics(
            MetricGroup consumerMetricGroup,
            Collection<PscTopicUriPartitionState<T, TUPH>> partitionOffsetStates) {

        for (PscTopicUriPartitionState<T, TUPH> ktp : partitionOffsetStates) {
            MetricGroup topicPartitionGroup = consumerMetricGroup
                    .addGroup(OFFSETS_BY_TOPIC_METRICS_GROUP, ktp.getTopicUri())
                    .addGroup(OFFSETS_BY_PARTITION_METRICS_GROUP, Integer.toString(ktp.getPscTopicUriPartition().getPartition()));

            topicPartitionGroup.gauge(CURRENT_OFFSETS_METRICS_GAUGE, new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
            topicPartitionGroup.gauge(COMMITTED_OFFSETS_METRICS_GAUGE, new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));

            legacyCurrentOffsetsMetricGroup.gauge(getLegacyOffsetsMetricsGaugeName(ktp), new OffsetGauge(ktp, OffsetGaugeType.CURRENT_OFFSET));
            legacyCommittedOffsetsMetricGroup.gauge(getLegacyOffsetsMetricsGaugeName(ktp), new OffsetGauge(ktp, OffsetGaugeType.COMMITTED_OFFSET));
        }
    }

    private static String getLegacyOffsetsMetricsGaugeName(PscTopicUriPartitionState<?, ?> ktp) {
        return ktp.getTopicUri() + "-" + ktp.getPscTopicUriPartition().getPartition();
    }

    /**
     * Gauge types.
     */
    private enum OffsetGaugeType {
        CURRENT_OFFSET,
        COMMITTED_OFFSET
    }

    /**
     * Gauge for getting the offset of a PscTopicUriPartitionState.
     */
    private static class OffsetGauge implements Gauge<Long> {

        private final PscTopicUriPartitionState<?, ?> ktp;
        private final OffsetGaugeType gaugeType;

        OffsetGauge(PscTopicUriPartitionState<?, ?> ktp, OffsetGaugeType gaugeType) {
            this.ktp = ktp;
            this.gaugeType = gaugeType;
        }

        @Override
        public Long getValue() {
            switch (gaugeType) {
                case COMMITTED_OFFSET:
                    return ktp.getCommittedOffset();
                case CURRENT_OFFSET:
                    return ktp.getOffset();
                default:
                    throw new RuntimeException("Unknown gauge type: " + gaugeType);
            }
        }
    }
    // ------------------------------------------------------------------------

    /**
     * The periodic watermark emitter. In its given interval, it checks all partitions for
     * the current event time watermark, and possibly emits the next watermark.
     */
    private static class PeriodicWatermarkEmitter<T, KPH> implements ProcessingTimeCallback {

        private final Object checkpointLock;

        private final Collection<PscTopicUriPartitionState<T, KPH>> allPartitions;

        private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;

        private final ProcessingTimeService timerService;

        private final long interval;

        //-------------------------------------------------

        PeriodicWatermarkEmitter(
                Object checkpointLock,
                Collection<PscTopicUriPartitionState<T, KPH>> allPartitions,
                WatermarkOutputMultiplexer watermarkOutputMultiplexer,
                ProcessingTimeService timerService,
                long autoWatermarkInterval) {
            this.checkpointLock = checkpointLock;
            this.allPartitions = checkNotNull(allPartitions);
            this.watermarkOutputMultiplexer = watermarkOutputMultiplexer;
            this.timerService = checkNotNull(timerService);
            this.interval = autoWatermarkInterval;
        }

        //-------------------------------------------------

        public void start() {
            timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
        }

        @Override
        public void onProcessingTime(long timestamp) {

            synchronized (checkpointLock) {
                for (PscTopicUriPartitionState<?, ?> state : allPartitions) {
                    state.onPeriodicEmit();
                }

                watermarkOutputMultiplexer.onPeriodicEmit();
            }

            // schedule the next watermark
            timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
        }
    }
}
