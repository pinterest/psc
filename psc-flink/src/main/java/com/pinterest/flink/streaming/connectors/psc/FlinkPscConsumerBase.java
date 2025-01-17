/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.streaming.connectors.psc.config.OffsetCommitMode;
import com.pinterest.flink.streaming.connectors.psc.config.OffsetCommitModes;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import com.pinterest.flink.streaming.connectors.psc.internals.AbstractFetcher;
import com.pinterest.flink.streaming.connectors.psc.internals.AbstractTopicUriPartitionDiscoverer;
import com.pinterest.flink.streaming.connectors.psc.internals.PscCommitCallback;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartitionAssigner;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartitionStateSentinel;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUrisDescriptor;
import com.pinterest.flink.streaming.connectors.psc.internals.metrics.FlinkPscStateRecoveryMetricConstants;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StateMigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.COMMITS_FAILED_METRICS_COUNTER;
import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.COMMITS_SUCCEEDED_METRICS_COUNTER;
import static com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscConsumerMetricConstants.PSC_CONSUMER_METRICS_GROUP;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class of all Flink PSC Consumer data sources.
 * This implements the common behavior across all PSC versions.
 *
 * <p>The PSC version specific behavior is defined mainly in the specific subclasses of the
 * {@link AbstractFetcher}.
 *
 * @param <T> The type of records produced by this data source
 */
@Internal
public abstract class FlinkPscConsumerBase<T> extends RichParallelSourceFunction<T> implements
        CheckpointListener,
        ResultTypeQueryable<T>,
        CheckpointedFunction {

    private static final long serialVersionUID = -6272159445203409112L;

    protected static final Logger LOG = LoggerFactory.getLogger(FlinkPscConsumerBase.class);

    /**
     * The maximum number of pending non-committed checkpoints to track, to avoid memory leaks.
     */
    public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

    /**
     * The default interval to execute partition discovery,
     * in milliseconds ({@code Long.MIN_VALUE}, i.e. disabled by default).
     */
    public static final long PARTITION_DISCOVERY_DISABLED = Long.MIN_VALUE;

    /**
     * Boolean configuration key to disable metrics tracking.
     **/
    public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

    /**
     * Configuration key to define the consumer's partition discovery interval, in milliseconds.
     */
    public static final String KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS = "flink.partition-discovery.interval-millis";

    /**
     * State name of the consumer's partition offset states.
     */
    @VisibleForTesting
    protected static final String OFFSETS_STATE_NAME = "topic-uri-partition-offset-states";

    // ------------------------------------------------------------------------
    //  configuration state, set on the client relevant for all subtasks
    // ------------------------------------------------------------------------

    /**
     * Describes whether we are discovering partitions for fixed topics or a topic pattern.
     */
    private final PscTopicUrisDescriptor topicUrisDescriptor;

    /**
     * The schema to convert between PSC's byte messages, and Flink's objects.
     */
    protected final PscDeserializationSchema<T> deserializer;

    /**
     * The set of topic partitions that the source will read, with their initial offsets to start reading from.
     */
    private Map<PscTopicUriPartition, Long> subscribedTopicUriPartitionsToStartOffsets;

    /**
     * Optional watermark strategy that will be run per PSC topic URI partition, to exploit per-partition
     * timestamp characteristics. The watermark strategy is kept in serialized form, to deserialize
     * it into multiple copies.
     */
    private SerializedValue<WatermarkStrategy<T>> watermarkStrategy;

    /**
     * User-set flag determining whether or not to commit on checkpoints.
     * Note: this flag does not represent the final offset commit mode.
     */
    private boolean enableCommitOnCheckpoints = true;

    /**
     * User-set flag to disable filtering restored partitions with current topics descriptor.
     */
    private boolean filterRestoredPartitionsWithCurrentTopicsDescriptor = true;

    /**
     * The offset commit mode for the consumer.
     * The value of this can only be determined in {@link FlinkPscConsumerBase#open(Configuration)} since it depends
     * on whether or not checkpointing is enabled for the job.
     */
    private OffsetCommitMode offsetCommitMode;

    /**
     * User configured value for discovery interval, in milliseconds.
     */
    private final long discoveryIntervalMillis;

    /**
     * The startup mode for the consumer (default is {@link StartupMode#GROUP_OFFSETS}).
     */
    private StartupMode startupMode = StartupMode.GROUP_OFFSETS;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}.
     */
    private Map<PscTopicUriPartition, Long> specificStartupOffsets;

    /**
     * Timestamp to determine startup offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.
     */
    private Long startupOffsetsTimestamp;

    // ------------------------------------------------------------------------
    //  runtime state (used individually by each parallel subtask)
    // ------------------------------------------------------------------------

    /**
     * Data for pending but uncommitted offsets.
     */
    private final LinkedMap pendingOffsetsToCommit = new LinkedMap();

    /**
     * The fetcher implements the connections to the backend pubsub.
     */
    private transient volatile AbstractFetcher<T, ?> pscFetcher;

    /**
     * The partition discoverer, used to find new partitions.
     */
    private transient volatile AbstractTopicUriPartitionDiscoverer topicUriPartitionDiscoverer;

    /**
     * The offsets to restore to, if the consumer restores state from a checkpoint.
     *
     * <p>This map will be populated by the {@link #initializeState(FunctionInitializationContext)} method.
     *
     * <p>Using a sorted map as the ordering is important when using restored state
     * to seed the partition discoverer.
     */
    private transient volatile TreeMap<PscTopicUriPartition, Long> restoredState;

    /**
     * Accessor for state in the operator state backend.
     */
    private transient ListState<Tuple2<PscTopicUriPartition, Long>> unionOffsetStates;

    /**
     * Discovery loop, executed in a separate thread.
     */
    private transient volatile Thread discoveryLoopThread;

    /**
     * Flag indicating whether the consumer is still running.
     */
    private volatile boolean running = true;

    // ------------------------------------------------------------------------
    //  internal metrics
    // ------------------------------------------------------------------------

    /**
     * Flag indicating whether or not metrics should be exposed.
     * If {@code true}, offset metrics (e.g. current offset, committed offset) and
     * PSC-shipped metrics will be registered.
     */
    private final boolean useMetrics;

    /**
     * Counter for successful PSC offset commits.
     */
    private transient Counter successfulCommits;

    /**
     * Counter for failed PSC offset commits.
     */
    private transient Counter failedCommits;

    /**
     * Callback interface that will be invoked upon async PSC commit completion.
     * Please be aware that default callback implementation in base class does not
     * provide any guarantees on thread-safety. This is sufficient for now because current
     * supported PSC connectors guarantee no more than 1 concurrent async pending offset
     * commit.
     */
    private transient PscCommitCallback offsetCommitCallback;

    protected final Properties properties;

    protected transient PscConfigurationInternal pscConfigurationInternal;

    private transient AtomicBoolean pscMetricsInitialized;

    // ------------------------------------------------------------------------

    /**
     * Base constructor.
     *
     * @param topicUris               fixed list of topics to subscribe to (null, if using topic pattern)
     * @param topicUriPattern         the topic pattern to subscribe to (null, if using fixed topics)
     * @param deserializer            The deserializer to turn raw byte messages into Java/Scala objects.
     * @param discoveryIntervalMillis the topic / partition discovery interval, in
     *                                milliseconds (0 if discovery is disabled).
     */
    public FlinkPscConsumerBase(
            List<String> topicUris,
            Pattern topicUriPattern,
            PscDeserializationSchema<T> deserializer,
            long discoveryIntervalMillis,
            boolean useMetrics,
            Properties props) {
        this.topicUrisDescriptor = new PscTopicUrisDescriptor(topicUris, topicUriPattern);
        this.deserializer = checkNotNull(deserializer, "valueDeserializer");
        this.properties = checkNotNull(props);

        checkArgument(
                discoveryIntervalMillis == PARTITION_DISCOVERY_DISABLED || discoveryIntervalMillis >= 0,
                "Cannot define a negative value for the topic / partition discovery interval.");
        this.discoveryIntervalMillis = discoveryIntervalMillis;

        this.useMetrics = useMetrics;

        initializePscConfigurationInternal();
        initializePscMetrics();
    }

    /**
     * Make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS.
     * This overwrites whatever setting the user configured in the properties.
     *
     * @param properties       - PSC configuration properties to be adjusted
     * @param offsetCommitMode offset commit mode
     */
    protected static void adjustAutoCommitConfig(Properties properties, OffsetCommitMode offsetCommitMode) {
        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS || offsetCommitMode == OffsetCommitMode.DISABLED) {
            properties.setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false");
        }
    }
    // ------------------------------------------------------------------------
    //  Configuration
    // ------------------------------------------------------------------------

    /**
     * Sets the given {@link WatermarkStrategy} on this consumer. These will be used to assign
     * timestamps to records and generates watermarks to signal event time progress.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the PSC source
     * (which you can do by using this method), per PSC topic URI partition, allows users to let them
     * exploit the per-partition characteristics.
     *
     * <p>When a subtask of a FlinkPscConsumer source reads multiple PSC topic URI partitions,
     * the streams from the partitions are unioned in a "first come first serve" fashion.
     * Per-partition characteristics are usually lost that way. For example, if the timestamps are
     * strictly ascending per PSC topic URI partition, they will not be strictly ascending in the resulting
     * Flink DataStream, if the parallel source subtask reads more than one partition.
     *
     * <p>Common watermark generation patterns can be found as static methods in the
     * {@link WatermarkStrategy} class.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkPscConsumerBase<T> assignTimestampsAndWatermarks(
            WatermarkStrategy<T> watermarkStrategy) {
        checkNotNull(watermarkStrategy);

        try {
            ClosureCleaner.clean(watermarkStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            this.watermarkStrategy = new SerializedValue<>(watermarkStrategy);
        } catch (Exception e) {
            throw new IllegalArgumentException("The given WatermarkStrategy is not serializable", e);
        }

        return this;
    }

    /**
     * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated manner.
     * The watermark extractor will run per PSC topic URI partition, watermarks will be merged across partitions
     * in the same way as in the Flink runtime, when streams are merged.
     *
     * <p>When a subtask of a FlinkPscConsumer source reads multiple PSC topic URI partitions,
     * the streams from the partitions are unioned in a "first come first serve" fashion. Per-partition
     * characteristics are usually lost that way. For example, if the timestamps are strictly ascending
     * per PSC topic URI partition, they will not be strictly ascending in the resulting Flink DataStream, if the
     * parallel source subtask reads more than one partition.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the PSC source, per PSC
     * topic URI partition, allows users to let them exploit the per-partition characteristics.
     *
     * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
     * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
     *
     * <p>This method uses the deprecated watermark generator interfaces. Please switch to
     * {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} to use the
     * new interfaces instead. The new interfaces support watermark idleness and no longer need
     * to differentiate between "periodic" and "punctuated" watermarks.
     *
     * @param assigner The timestamp assigner / watermark generator to use.
     * @return The consumer object, to allow function chaining.
     * @deprecated Please use {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} instead.
     */
    @Deprecated
    public FlinkPscConsumerBase<T> assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks<T> assigner) {
        checkNotNull(assigner);

        if (this.watermarkStrategy != null) {
            throw new IllegalStateException("Some watermark strategy has already been set.");
        }

        try {
            ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            final WatermarkStrategy<T> wms = new AssignerWithPunctuatedWatermarksAdapter.Strategy<>(assigner);

            return assignTimestampsAndWatermarks(wms);
        } catch (Exception e) {
            throw new IllegalArgumentException("The given assigner is not serializable", e);
        }
    }

    /**
     * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated manner.
     * The watermark extractor will run per PSC topic URI partition, watermarks will be merged across partitions
     * in the same way as in the Flink runtime, when streams are merged.
     *
     * <p>When a subtask of a FlinkPscConsumer source reads multiple PSC topic URI partitions,
     * the streams from the partitions are unioned in a "first come first serve" fashion. Per-partition
     * characteristics are usually lost that way. For example, if the timestamps are strictly ascending
     * per PSC topic URI partition, they will not be strictly ascending in the resulting Flink DataStream, if the
     * parallel source subtask reads more that one partition.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the PSC source, per PSC
     * topic URI partition, allows users to let them exploit the per-partition characteristics.
     *
     * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an
     * {@link AssignerWithPeriodicWatermarks}, not both at the same time.
     *
     * <p>This method uses the deprecated watermark generator interfaces. Please switch to
     * {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} to use the
     * new interfaces instead. The new interfaces support watermark idleness and no longer need
     * to differentiate between "periodic" and "punctuated" watermarks.
     *
     * @param assigner The timestamp assigner / watermark generator to use.
     * @return The consumer object, to allow function chaining.
     * @deprecated Please use {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} instead.
     */
    @Deprecated
    public FlinkPscConsumerBase<T> assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T> assigner) {
        checkNotNull(assigner);

        if (this.watermarkStrategy != null) {
            throw new IllegalStateException("Some watermark strategy has already been set.");
        }

        try {
            ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            final WatermarkStrategy<T> wms = new AssignerWithPeriodicWatermarksAdapter.Strategy<>(assigner);

            return assignTimestampsAndWatermarks(wms);
        } catch (Exception e) {
            throw new IllegalArgumentException("The given assigner is not serializable", e);
        }
    }

    /**
     * Specifies whether or not the consumer should commit offsets back on checkpoints.
     *
     * <p>This setting will only have effect if checkpointing is enabled for the job.
     * If checkpointing isn't enabled, only the "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
     * property settings will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkPscConsumerBase<T> setCommitOffsetsOnCheckpoints(boolean commitOnCheckpoints) {
        this.enableCommitOnCheckpoints = commitOnCheckpoints;
        return this;
    }

    /**
     * Specifies the consumer to start reading from the earliest offset for all partitions.
     * This lets the consumer ignore any committed group offsets.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
     * savepoint, only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkPscConsumerBase<T> setStartFromEarliest() {
        this.startupMode = StartupMode.EARLIEST;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading from the latest offset for all partitions.
     * This lets the consumer ignore any committed group offsets.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
     * savepoint, only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkPscConsumerBase<T> setStartFromLatest() {
        this.startupMode = StartupMode.LATEST;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading partitions from a specified timestamp.
     * The specified timestamp must be before the current timestamp.
     * This lets the consumer ignore any committed group offsets.
     *
     * <p>The consumer will look up the earliest offset whose timestamp is greater than or equal
     * to the specific timestamp from the pubsub. If there's no such offset, the consumer will use the
     * latest offset to read data from the pubsub.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
     * savepoint, only the offsets in the restored state will be used.
     *
     * @param startupOffsetsTimestamp timestamp for the startup offsets, as milliseconds from epoch.
     * @return The consumer object, to allow function chaining.
     */
    public FlinkPscConsumerBase<T> setStartFromTimestamp(long startupOffsetsTimestamp) {
        checkArgument(startupOffsetsTimestamp >= 0, "The provided value for the startup offsets timestamp is invalid.");

        long currentTimestamp = System.currentTimeMillis();
        checkArgument(startupOffsetsTimestamp <= currentTimestamp,
                "Startup time[%s] must be before current time[%s].", startupOffsetsTimestamp, currentTimestamp);

        this.startupMode = StartupMode.TIMESTAMP;
        this.startupOffsetsTimestamp = startupOffsetsTimestamp;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading from any committed group offsets.
     * The "group.id" property must be set in the configuration
     * properties. If no offset can be found for a partition, the behaviour in "auto.offset.reset"
     * set in the configuration properties will be used for the partition.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
     * savepoint, only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkPscConsumerBase<T> setStartFromGroupOffsets() {
        this.startupMode = StartupMode.GROUP_OFFSETS;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading partitions from specific offsets, set independently for each partition.
     * The specified offset should be the offset of the next record that will be read from partitions.
     * This lets the consumer ignore any committed group offsets.
     *
     * <p>If the provided map of offsets contains entries whose {@link PscTopicUriPartition} is not subscribed by the
     * consumer, the entry will be ignored. If the consumer subscribes to a partition that does not exist in the provided
     * map of offsets, the consumer will fallback to the default group offset behaviour (see
     * {@link FlinkPscConsumerBase#setStartFromGroupOffsets()}) for that particular partition.
     *
     * <p>If the specified offset for a partition is invalid, or the behaviour for that partition is defaulted to group
     * offsets but still no group offset could be found for it, then the "auto.offset.reset" behaviour set in the
     * configuration properties will be used for the partition
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
     * savepoint, only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkPscConsumerBase<T> setStartFromSpecificOffsets(Map<PscTopicUriPartition, Long> specificStartupOffsets) {
        this.startupMode = StartupMode.SPECIFIC_OFFSETS;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = checkNotNull(specificStartupOffsets);
        return this;
    }

    /**
     * By default, when restoring from a checkpoint / savepoint, the consumer always
     * ignores restored partitions that are no longer associated with the current specified topics or
     * topic pattern to subscribe to.
     *
     * <p>This method configures the consumer to not filter the restored partitions,
     * therefore always attempting to consume whatever partition was present in the
     * previous execution regardless of the specified topics to subscribe to in the
     * current execution.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkPscConsumerBase<T> disableFilterRestoredPartitionsWithSubscribedTopics() {
        this.filterRestoredPartitionsWithCurrentTopicsDescriptor = false;
        return this;
    }

    // ------------------------------------------------------------------------
    //  Work methods
    // ------------------------------------------------------------------------

    @Override
    public void open(Configuration configuration) throws Exception {
        // determine the offset commit mode
        this.offsetCommitMode = OffsetCommitModes.fromConfiguration(
                getIsAutoCommitEnabled(),
                enableCommitOnCheckpoints,
                ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled());

        // create the partition discoverer
        this.topicUriPartitionDiscoverer = createTopicUriPartitionDiscoverer(
                topicUrisDescriptor,
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getNumberOfParallelSubtasks());
        this.topicUriPartitionDiscoverer.open();

        subscribedTopicUriPartitionsToStartOffsets = new HashMap<>();
        final List<PscTopicUriPartition> allPartitions = topicUriPartitionDiscoverer.discoverPartitions();
        if (restoredState != null) {
            for (PscTopicUriPartition partition : allPartitions) {
                if (!restoredState.containsKey(partition)) {
                    restoredState.put(partition, PscTopicUriPartitionStateSentinel.EARLIEST_OFFSET);
                }
            }

            for (Map.Entry<PscTopicUriPartition, Long> restoredStateEntry : restoredState.entrySet()) {
                // seed the partition discoverer with the union state while filtering out
                // restored partitions that should not be subscribed by this subtask
                if (PscTopicUriPartitionAssigner.assign(
                        restoredStateEntry.getKey(), getRuntimeContext().getNumberOfParallelSubtasks())
                        == getRuntimeContext().getIndexOfThisSubtask()) {
                    subscribedTopicUriPartitionsToStartOffsets.put(restoredStateEntry.getKey(), restoredStateEntry.getValue());
                }
            }

            if (filterRestoredPartitionsWithCurrentTopicsDescriptor) {
                subscribedTopicUriPartitionsToStartOffsets.entrySet().removeIf(entry -> {
                    if (!topicUrisDescriptor.isMatchingTopicUri(entry.getKey().getTopicUriStr())) {
                        LOG.warn(
                                "{} is removed from subscribed partitions since it is no longer associated with topics descriptor of current execution.",
                                entry.getKey());
                        return true;
                    }
                    return false;
                });
            }

            LOG.info("Consumer subtask {} will start reading {} partitions with offsets in restored state: {}",
                    getRuntimeContext().getIndexOfThisSubtask(), subscribedTopicUriPartitionsToStartOffsets.size(), subscribedTopicUriPartitionsToStartOffsets);
        } else {
            // use the partition discoverer to fetch the initial seed partitions,
            // and set their initial offsets depending on the startup mode.
            // for SPECIFIC_OFFSETS and TIMESTAMP modes, we set the specific offsets now;
            // for other modes (EARLIEST, LATEST, and GROUP_OFFSETS), the offset is lazily determined
            // when the partition is actually read.
            switch (startupMode) {
                case SPECIFIC_OFFSETS:
                    if (specificStartupOffsets == null) {
                        throw new IllegalStateException(
                                "Startup mode for the consumer set to " + StartupMode.SPECIFIC_OFFSETS +
                                        ", but no specific offsets were specified.");
                    }

                    for (PscTopicUriPartition seedPartition : allPartitions) {
                        Long specificOffset = specificStartupOffsets.get(seedPartition);
                        if (specificOffset != null) {
                            // since the specified offsets represent the next record to read, we subtract
                            // it by one so that the initial state of the consumer will be correct
                            subscribedTopicUriPartitionsToStartOffsets.put(seedPartition, specificOffset - 1);
                        } else {
                            // default to group offset behaviour if the user-provided specific offsets
                            // do not contain a value for this partition
                            subscribedTopicUriPartitionsToStartOffsets.put(seedPartition, PscTopicUriPartitionStateSentinel.GROUP_OFFSET);
                        }
                    }

                    break;
                case TIMESTAMP:
                    if (startupOffsetsTimestamp == null) {
                        throw new IllegalStateException(
                                "Startup mode for the consumer set to " + StartupMode.TIMESTAMP +
                                        ", but no startup timestamp was specified.");
                    }

                    for (Map.Entry<PscTopicUriPartition, Long> partitionToOffset
                            : fetchOffsetsWithTimestamp(allPartitions, startupOffsetsTimestamp).entrySet()) {
                        subscribedTopicUriPartitionsToStartOffsets.put(
                                partitionToOffset.getKey(),
                                (partitionToOffset.getValue() == null)
                                        // if an offset cannot be retrieved for a partition with the given timestamp,
                                        // we default to using the latest offset for the partition
                                        ? PscTopicUriPartitionStateSentinel.LATEST_OFFSET
                                        // since the specified offsets represent the next record to read, we subtract
                                        // it by one so that the initial state of the consumer will be correct
                                        : partitionToOffset.getValue() - 1);
                    }

                    break;
                default:
                    for (PscTopicUriPartition seedPartition : allPartitions) {
                        subscribedTopicUriPartitionsToStartOffsets.put(seedPartition, startupMode.getStateSentinel());
                    }
            }

            if (!subscribedTopicUriPartitionsToStartOffsets.isEmpty()) {
                switch (startupMode) {
                    case EARLIEST:
                        LOG.info("Consumer subtask {} will start reading the following {} partitions from the earliest offsets: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedTopicUriPartitionsToStartOffsets.size(),
                                subscribedTopicUriPartitionsToStartOffsets.keySet());
                        break;
                    case LATEST:
                        LOG.info("Consumer subtask {} will start reading the following {} partitions from the latest offsets: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedTopicUriPartitionsToStartOffsets.size(),
                                subscribedTopicUriPartitionsToStartOffsets.keySet());
                        break;
                    case TIMESTAMP:
                        LOG.info("Consumer subtask {} will start reading the following {} partitions from timestamp {}: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedTopicUriPartitionsToStartOffsets.size(),
                                startupOffsetsTimestamp,
                                subscribedTopicUriPartitionsToStartOffsets.keySet());
                        break;
                    case SPECIFIC_OFFSETS:
                        LOG.info("Consumer subtask {} will start reading the following {} partitions from the specified startup offsets {}: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedTopicUriPartitionsToStartOffsets.size(),
                                specificStartupOffsets,
                                subscribedTopicUriPartitionsToStartOffsets.keySet());

                        List<PscTopicUriPartition> partitionsDefaultedToGroupOffsets = new ArrayList<>(subscribedTopicUriPartitionsToStartOffsets.size());
                        for (Map.Entry<PscTopicUriPartition, Long> subscribedPartition : subscribedTopicUriPartitionsToStartOffsets.entrySet()) {
                            if (subscribedPartition.getValue() == PscTopicUriPartitionStateSentinel.GROUP_OFFSET) {
                                partitionsDefaultedToGroupOffsets.add(subscribedPartition.getKey());
                            }
                        }

                        if (partitionsDefaultedToGroupOffsets.size() > 0) {
                            LOG.warn("Consumer subtask {} cannot find offsets for the following {} partitions in the specified startup offsets: {}" +
                                            "; their startup offsets will be defaulted to their committed group offsets.",
                                    getRuntimeContext().getIndexOfThisSubtask(),
                                    partitionsDefaultedToGroupOffsets.size(),
                                    partitionsDefaultedToGroupOffsets);
                        }
                        break;
                    case GROUP_OFFSETS:
                        LOG.info("Consumer subtask {} will start reading the following {} partitions from the committed group offsets: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedTopicUriPartitionsToStartOffsets.size(),
                                subscribedTopicUriPartitionsToStartOffsets.keySet());
                }
            } else {
                LOG.info("Consumer subtask {} initially has no partitions to read from.",
                        getRuntimeContext().getIndexOfThisSubtask());
            }
        }
        this.deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(this.getRuntimeContext(), (metricGroup) -> {
            return metricGroup.addGroup("user");
        }));
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        if (subscribedTopicUriPartitionsToStartOffsets == null) {
            throw new Exception("The partitions were not set for the consumer");
        }

        // initialize commit metrics and default offset callback method
        this.successfulCommits = this.getRuntimeContext().getMetricGroup().counter(COMMITS_SUCCEEDED_METRICS_COUNTER);
        this.failedCommits = this.getRuntimeContext().getMetricGroup().counter(COMMITS_FAILED_METRICS_COUNTER);
        final int subtaskIndex = this.getRuntimeContext().getIndexOfThisSubtask();

        this.offsetCommitCallback = new PscCommitCallback() {
            @Override
            public void onSuccess() {
                successfulCommits.inc();
            }

            @Override
            public void onException(Throwable cause) {
                LOG.warn(String.format("Consumer subtask %d failed async commit.", subtaskIndex), cause);
                failedCommits.inc();
            }
        };

        // mark the subtask as temporarily idle if there are no initial seed partitions;
        // once this subtask discovers some partitions and starts collecting records, the subtask's
        // status will automatically be triggered back to be active.
        if (subscribedTopicUriPartitionsToStartOffsets.isEmpty()) {
            sourceContext.markAsTemporarilyIdle();
        }

        LOG.info("Consumer subtask {} creating fetcher with offsets {}.",
                getRuntimeContext().getIndexOfThisSubtask(), subscribedTopicUriPartitionsToStartOffsets);
        // from this point forward:
        //   - 'snapshotState' will draw offsets from the fetcher,
        //     instead of being built from `subscribedPartitionsToStartOffsets`
        //   - 'notifyCheckpointComplete' will start to do work (i.e. commit offsets
        //     through the fetcher, if configured to do so)
        this.pscFetcher = createFetcher(
                sourceContext,
                subscribedTopicUriPartitionsToStartOffsets,
                watermarkStrategy,
                (StreamingRuntimeContext) getRuntimeContext(),
                offsetCommitMode,
                getRuntimeContext().getMetricGroup().addGroup(PSC_CONSUMER_METRICS_GROUP),
                useMetrics
            );

        if (!running) {
            return;
        }

        // depending on whether we were restored with the current state version (1.3),
        // remaining logic branches off into 2 paths:
        //  1) New state - partition discovery loop executed as separate thread, with this
        //                 thread running the main fetcher loop
        //  2) Old state - partition discovery is disabled and only the main fetcher loop is executed
        if (discoveryIntervalMillis == PARTITION_DISCOVERY_DISABLED) {
            LOG.info("Starting FlinkPscConsumerBase without partition discovery.");
            pscFetcher.runFetchLoop();
        } else {
            LOG.info("Starting FlinkPscConsumerBase with partition discovery interval={}ms.", discoveryIntervalMillis);
            runWithPartitionDiscovery();
        }
    }

    private void runWithPartitionDiscovery() throws Exception {
        final AtomicReference<Exception> discoveryLoopErrorRef = new AtomicReference<>();
        createAndStartDiscoveryLoop(discoveryLoopErrorRef);

        pscFetcher.runFetchLoop();

        // make sure that the partition discoverer is waked up so that
        // the discoveryLoopThread exits
        topicUriPartitionDiscoverer.wakeup();
        joinDiscoveryLoopThread();

        // rethrow any fetcher errors
        final Exception discoveryLoopError = discoveryLoopErrorRef.get();
        if (discoveryLoopError != null) {
            throw new RuntimeException(discoveryLoopError);
        }
    }

    @VisibleForTesting
    void joinDiscoveryLoopThread() throws InterruptedException {
        if (discoveryLoopThread != null) {
            discoveryLoopThread.join();
        }
    }

    private void createAndStartDiscoveryLoop(AtomicReference<Exception> discoveryLoopErrorRef) {
        discoveryLoopThread = new Thread(() -> {
            try {
                // --------------------- partition discovery loop ---------------------

                // throughout the loop, we always eagerly check if we are still running before
                // performing the next operation, so that we can escape the loop as soon as possible

                while (running) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Consumer subtask {} is trying to discover new partitions ...", getRuntimeContext().getIndexOfThisSubtask());
                    }

                    final List<PscTopicUriPartition> discoveredPartitions;
                    try {
                        discoveredPartitions = topicUriPartitionDiscoverer.discoverPartitions();
                    } catch (AbstractTopicUriPartitionDiscoverer.WakeupException | AbstractTopicUriPartitionDiscoverer.ClosedException e) {
                        // the partition discoverer may have been closed or woken up before or during the discovery;
                        // this would only happen if the consumer was canceled; simply escape the loop
                        break;
                    }

                    // no need to add the discovered partitions if we were closed during the meantime
                    if (running && !discoveredPartitions.isEmpty()) {
                        LOG.info("Consumer subtask {} discovered {} new partitions: {}",
                                getRuntimeContext().getIndexOfThisSubtask(), discoveredPartitions.size(), discoveredPartitions);
                        pscFetcher.addDiscoveredPartitions(discoveredPartitions);
                    }

                    // do not waste any time sleeping if we're not running anymore
                    if (running && discoveryIntervalMillis != 0) {
                        try {
                            Thread.sleep(discoveryIntervalMillis);
                        } catch (InterruptedException iex) {
                            // may be interrupted if the consumer was canceled midway; simply escape the loop
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                discoveryLoopErrorRef.set(e);
            } finally {
                // calling cancel will also let the fetcher loop escape
                // (if not running, cancel() was already called)
                if (running) {
                    cancel();
                }
            }
        }, "PSC Topic URI Partition Discovery for " + getRuntimeContext().getTaskNameWithSubtasks());

        discoveryLoopThread.start();
    }

    @Override
    public void cancel() {
        // set ourselves as not running;
        // this would let the main discovery loop escape as soon as possible
        running = false;

        if (discoveryLoopThread != null) {

            if (topicUriPartitionDiscoverer != null) {
                // we cannot close the discoverer here, as it is error-prone to concurrent access;
                // only wakeup the discoverer, the discovery loop will clean itself up after it escapes
                topicUriPartitionDiscoverer.wakeup();
            }

            // the discovery loop may currently be sleeping in-between
            // consecutive discoveries; interrupt to shutdown faster
            discoveryLoopThread.interrupt();
        }

        // abort the fetcher, if there is one
        if (pscFetcher != null) {
            pscFetcher.cancel();
        }
    }

    @Override
    public void close() throws Exception {
        cancel();

        joinDiscoveryLoopThread();

        Exception exception = null;
        if (topicUriPartitionDiscoverer != null) {
            try {
                topicUriPartitionDiscoverer.close();
            } catch (Exception e) {
                exception = e;
            }
        }

        try {
            super.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }

    protected void initializePscMetrics() {

        if (pscMetricsInitialized == null)
            pscMetricsInitialized = new AtomicBoolean(false);

        if (pscMetricsInitialized.compareAndSet(false, true)) {
            PscMetricRegistryManager.getInstance().initialize(pscConfigurationInternal);
        }
    }

    protected void initializePscConfigurationInternal() {
        if (pscConfigurationInternal == null) {
            pscConfigurationInternal = PscConfigurationUtils.propertiesToPscConfigurationInternal(properties, PscConfiguration.PSC_CLIENT_TYPE_CONSUMER);
        }
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and restore
    // ------------------------------------------------------------------------

    @Override
    public final void initializeState(FunctionInitializationContext context) throws Exception {
        initializePscConfigurationInternal();
        initializePscMetrics();


        // Entry point for checkpoint load
        // First try loading a flink-psc checkpoint
        OperatorStateStore stateStore = context.getOperatorStateStore();

        /*
        The current snapshot recovery logic works like this:

        1. If there is a PSC key (topic-uri-partition-offset-states) in the state, it's content will be used for state
           recovery.
        2. If there is no PSC key (topic-uri-partition-offset-states) in the state and there is a Kafka key
           (topic-partition-offset-states) in the state, the content of Kafka sub-state will be used and converted to a
           PSC sub-state for recovery.

        One scenario this could lead to unexpected outcome is when
        a. A Flink-Kafka based application produced some checkpoint with a topic-partition-offset-states sub-state.
        b. A Flink-PSC based application recovered that state and add a topic-uri-partition-offset-states sub-state.
        c. For some reason, the Flink-Kafka application was used again against the latest state (which has both
           topic-partition-offset-states and topic-uri-partition-offset-states sub-states), and bumped the offsets in
           topic-partition-offset-states sub-state.
        d. The Flink-PSC application was used to recover that state. This time, the Flink-PSC app would not read the
           latest sub-state (in topic-partition-offset-states), and resumes from the sub-state in
           topic-uri-partition-offset-states.

        To avoid this unexpected result, the behavior in the bullet #2 above can change to
        2. If there is no PSC key (topic-uri-partition-offset-states) in the state and there is a Kafka key
           (topic-partition-offset-states) in the state, the two sub-states will be compared and the one with more
           recent offsets will be used for recovery. If topic-partition-offset-states sub-state has more recent offsets,
           the topic-uri-partition-offset-states sub-state will be overwritten by those. Else, the
           topic-uri-partition-offset-states sub-state will be used for recovery (as in the first bullet).
        */

        if (stateStore.getRegisteredStateNames().contains(OFFSETS_STATE_NAME)) {
            LOG.info("Detected a flink-psc checkpoint.");
            this.unionOffsetStates = stateStore.getUnionListState(
                    new ListStateDescriptor<>(
                            OFFSETS_STATE_NAME,
                            createStateSerializer(getRuntimeContext().getExecutionConfig())
                    )
            );

            if (context.isRestored()) {
                try {
                    restoredState = new TreeMap<>(new PscTopicUriPartition.Comparator());

                    // populate actual holder for restored state
                    // while converting topic URIs in old schema versions to the current schema version, if necessary
                    for (Tuple2<PscTopicUriPartition, Long> pscTopicUriPartitionOffset : unionOffsetStates.get()) {
                        // may be on an old schema
                        PscTopicUriPartition pscTopicUriPartitionFromState = pscTopicUriPartitionOffset.f0;
                        TopicUri matchingTopicUri = getMatchingTopicUriForTopicUri(
                                pscTopicUriPartitionFromState.getTopicUri()
                        );
                        PscTopicUriPartition pscTopicUriPartition = new PscTopicUriPartition(
                                matchingTopicUri, pscTopicUriPartitionFromState.getPartition()
                        );
                        restoredState.put(pscTopicUriPartition, pscTopicUriPartitionOffset.f1);

                        PscMetricRegistryManager.getInstance().incrementCounterMetric(
                                pscTopicUriPartition.getTopicUri(),
                                pscTopicUriPartition.getPartition(),
                                FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_SUCCESS,
                                pscConfigurationInternal
                        );
                    }

                    LOG.info("Consumer subtask {} restored flink-psc state: {}.", getRuntimeContext().getIndexOfThisSubtask(), restoredState);
                    PscMetricRegistryManager.getInstance().incrementCounterMetric(
                            null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_SUCCESS, pscConfigurationInternal
                    );
                    PscMetricRegistryManager.getInstance().updateHistogramMetric(
                            null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_PARTITIONS, restoredState.size(), pscConfigurationInternal
                    );
                } catch (Exception e) {
                    LOG.error("Consumer subtask {} failed to restore from a flink-psc state.", getRuntimeContext().getIndexOfThisSubtask());
                    PscMetricRegistryManager.getInstance().incrementCounterMetric(
                            null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_FAILURE, pscConfigurationInternal
                    );
                    throw e;
                }
            } else {
                LOG.info("Consumer subtask {} has no restored flink-psc state.", getRuntimeContext().getIndexOfThisSubtask());
                PscMetricRegistryManager.getInstance().incrementCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_NONE, pscConfigurationInternal
                );
                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_PARTITIONS, 0, pscConfigurationInternal
                );
            }
        } else if (stateStore.getRegisteredStateNames().contains(
                (String) PscCommon.getField(FlinkKafkaConsumerBase.class, "OFFSETS_STATE_NAME")
        )) {
            LOG.info("Detected a flink-kafka checkpoint.");
            String kafkaOffsetStateName = (String) PscCommon.getField(FlinkKafkaConsumerBase.class, "OFFSETS_STATE_NAME");
            // If unsuccessful, fallback to loading a flink-kafka checkpoint
            ListState<Tuple2<KafkaTopicPartition, Long>> unionKafkaOffsetStates = stateStore.getUnionListState(
                    new ListStateDescriptor<>(
                            kafkaOffsetStateName,
                            (TupleSerializer<Tuple2<KafkaTopicPartition, Long>>) PscCommon.invoke(
                                    FlinkKafkaConsumerBase.class,
                                    "createStateSerializer",
                                    new Class[]{ExecutionConfig.class},
                                    new ExecutionConfig[]{getRuntimeContext().getExecutionConfig()}
                            )
                    )
            );

            this.unionOffsetStates = stateStore.getUnionListState(
                    new ListStateDescriptor<>(
                            OFFSETS_STATE_NAME,
                            createStateSerializer(getRuntimeContext().getExecutionConfig())
                    )
            );

            if (context.isRestored()) {
                try {
                    restoredState = new TreeMap<>(new PscTopicUriPartition.Comparator());
                    // populate actual holder for restored state
                    for (Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionOffset : unionKafkaOffsetStates.get()) {
                        String topic = kafkaTopicPartitionOffset.f0.getTopic();
                        int partition = kafkaTopicPartitionOffset.f0.getPartition();
                        long offset = kafkaTopicPartitionOffset.f1;

                        TopicUri matchingTopicUri = getMatchingTopicUriForKafkaTopic(topic);
                        if (matchingTopicUri == null) {
                            LOG.error("Unable to convert topic {} to a topic URI.", topic);
                            throw new StateMigrationException(String.format("Unable to convert topic %s to a topic URI.", topic));
                        }
                        PscTopicUriPartition pscTopicUriPartition = new PscTopicUriPartition(matchingTopicUri, partition);
                        restoredState.put(pscTopicUriPartition, offset);
                        this.unionOffsetStates.add(Tuple2.of(pscTopicUriPartition, offset));

                        LOG.info("State conversion done: ({}, {}) -> ({}, {}).",
                                kafkaTopicPartitionOffset.f0, offset, pscTopicUriPartition, offset
                        );

                        PscMetricRegistryManager.getInstance().incrementCounterMetric(
                                pscTopicUriPartition.getTopicUri(),
                                pscTopicUriPartition.getPartition(),
                                FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_SUCCESS, pscConfigurationInternal
                        );
                    }

                    stateStore.getRegisteredStateNames().remove(kafkaOffsetStateName);
                    unionKafkaOffsetStates.clear();
                    LOG.info("Consumer subtask {} restored state from flink-kafka state: {}.", getRuntimeContext().getIndexOfThisSubtask(), restoredState);
                    PscMetricRegistryManager.getInstance().incrementCounterMetric(
                            null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_SUCCESS, pscConfigurationInternal
                    );
                    PscMetricRegistryManager.getInstance().updateHistogramMetric(
                            null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_PARTITIONS, restoredState.size(), pscConfigurationInternal
                    );
                    PscMetricRegistryManager.getInstance().updateHistogramMetric(
                            null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_PARTITIONS, getSize(this.unionOffsetStates), pscConfigurationInternal
                    );
                } catch (Exception e) {
                    LOG.error("Consumer subtask {} failed to restore from a flink-kafka state.", getRuntimeContext().getIndexOfThisSubtask());
                    PscMetricRegistryManager.getInstance().incrementCounterMetric(
                            null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_FAILURE, pscConfigurationInternal
                    );
                    throw e;
                }
            } else {
                LOG.info("Consumer subtask {} has no restored flink-kafka state.", getRuntimeContext().getIndexOfThisSubtask());
                PscMetricRegistryManager.getInstance().incrementCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_NONE, pscConfigurationInternal
                );
                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_PARTITIONS, 0, pscConfigurationInternal
                );
                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_PARTITIONS, getSize(this.unionOffsetStates), pscConfigurationInternal
                );
            }
        } else {
            LOG.info("Consumer subtask {} is starting from a fresh state.", getRuntimeContext().getIndexOfThisSubtask());
            PscMetricRegistryManager.getInstance().incrementCounterMetric(
                    null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_FRESH, pscConfigurationInternal
            );
            PscMetricRegistryManager.getInstance().updateHistogramMetric(
                    null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_PARTITIONS, 0, pscConfigurationInternal
            );
            PscMetricRegistryManager.getInstance().updateHistogramMetric(
                    null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_PARTITIONS, 0, pscConfigurationInternal
            );
        }

        if (this.unionOffsetStates == null) {
            this.unionOffsetStates = stateStore.getUnionListState(
                    new ListStateDescriptor<>(
                            OFFSETS_STATE_NAME,
                            createStateSerializer(getRuntimeContext().getExecutionConfig())
                    )
            );
        }

        LOG.info("Restored state: {}", restoredState);
    }

    private long getSize(ListState<Tuple2<PscTopicUriPartition, Long>> unionOffsetStates) throws Exception {
        AtomicLong size = new AtomicLong(0);
        unionOffsetStates.get().iterator().forEachRemaining(item -> size.incrementAndGet());
        return size.get();
    }

    private TopicUri getMatchingTopicUriForKafkaTopic(String topic) throws TopicUriSyntaxException {
        List<String> topicUriStrs = topicUrisDescriptor.getFixedTopicUris();
        for (String topicUriStr : topicUriStrs) {
            TopicUri topicUri = TopicUri.validate(topicUriStr);
            if (topicUri.getTopic().equals(topic)) {
                LOG.info("Found matching topic URI '{}' for topic '{}'.", topicUriStr, topic);
                return topicUri;
            }
        }

        LOG.warn(
                "Offset state associated with topic '{}' does not match the given topic URI(s) '{}'.",
                topic,
                String.join(", ", topicUriStrs)
        );

        return null;
    }

    private TopicUri getMatchingTopicUriForTopicUri(TopicUri topicUriFromState) throws TopicUriSyntaxException {
        List<String> topicUriStrs = topicUrisDescriptor.getFixedTopicUris();
        for (String topicUriStr : topicUriStrs) {
            TopicUri topicUri = TopicUri.validate(topicUriStr);
            if (topicUri.getTopicRn().toString().equals(topicUriFromState.getTopicRn().toString())) {
                LOG.info("Found matching topic URI '{}' for topic RN '{}'.", topicUriStr, topicUriFromState);
                return topicUri;
            }
        }

        LOG.warn(
                "Offset state associated with topic RN '{}' does not match the given topic URI(s) '{}'.",
                topicUriFromState,
                String.join(", ", topicUriStrs)
        );

        return topicUriFromState;
    }

    @Override
    public final void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!running) {
            LOG.debug("snapshotState() called on closed source");
        } else {
            unionOffsetStates.clear();

            final AbstractFetcher<?, ?> fetcher = this.pscFetcher;
            if (fetcher == null) {
                // the fetcher has not yet been initialized, which means we need to return the
                // originally restored offsets or the assigned partitions
                for (Map.Entry<PscTopicUriPartition, Long> subscribedPartition : subscribedTopicUriPartitionsToStartOffsets.entrySet()) {
                    unionOffsetStates.add(Tuple2.of(subscribedPartition.getKey(), subscribedPartition.getValue()));
                }

                if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                    // the map cannot be asynchronously updated, because only one checkpoint call can happen
                    // on this function at a time: either snapshotState() or notifyCheckpointComplete()
                    pendingOffsetsToCommit.put(context.getCheckpointId(), restoredState);
                    if (restoredState != null) {
                        restoredState.forEach((key, value) ->
                                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                                        key.getTopicUri(),
                                        key.getPartition(),
                                        FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_SNAPSHOT_PSC_PENDING_OFFSETS,
                                        1,
                                        pscConfigurationInternal

                                )
                        );
                    }
                }
            } else {
                HashMap<PscTopicUriPartition, Long> currentOffsets = fetcher.snapshotCurrentState();

                if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                    // the map cannot be asynchronously updated, because only one checkpoint call can happen
                    // on this function at a time: either snapshotState() or notifyCheckpointComplete()
                    pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
                    if (currentOffsets != null) {
                        currentOffsets.forEach((key, value) ->
                                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                                        key.getTopicUri(),
                                        key.getPartition(),
                                        FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_SNAPSHOT_PSC_PENDING_OFFSETS,
                                        1,
                                        pscConfigurationInternal
                                )
                        );
                    }
                }

                for (Map.Entry<PscTopicUriPartition, Long> pscTopicUriPartitionOffset : currentOffsets.entrySet()) {
                    unionOffsetStates.add(
                            Tuple2.of(pscTopicUriPartitionOffset.getKey(), pscTopicUriPartitionOffset.getValue()));
                }
            }

            if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                // truncate the map of pending offsets to commit, to prevent infinite growth
                PscMetricRegistryManager.getInstance().updateHistogramMetric(
                        null,
                        FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_SNAPSHOT_PSC_REMOVED_PENDING_OFFSETS,
                        pendingOffsetsToCommit.size() - MAX_NUM_PENDING_CHECKPOINTS,
                        pscConfigurationInternal
                );
                while (pendingOffsetsToCommit.size() > MAX_NUM_PENDING_CHECKPOINTS) {
                    pendingOffsetsToCommit.remove(0);
                }
            }

            if (unionOffsetStates != null) {
                unionOffsetStates.get().forEach(state ->
                        PscMetricRegistryManager.getInstance().updateHistogramMetric(
                                state.f0.getTopicUri(),
                                state.f0.getPartition(),
                                FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_SNAPSHOT_PSC_OFFSETS,
                                1,
                                pscConfigurationInternal
                        )
                );
            }

            PscMetricRegistryManager.getInstance().updateHistogramMetric(
                    null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_SNAPSHOT_PSC_OFFSETS, getSize(unionOffsetStates), pscConfigurationInternal
            );
            PscMetricRegistryManager.getInstance().updateHistogramMetric(
                    null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_SNAPSHOT_PSC_PENDING_OFFSETS, pendingOffsetsToCommit.size(), pscConfigurationInternal
            );
        }
    }

    @Override
    public final void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!running) {
            LOG.debug("notifyCheckpointComplete() called on closed source");
            return;
        }

        final AbstractFetcher<?, ?> fetcher = this.pscFetcher;
        if (fetcher == null) {
            LOG.debug("notifyCheckpointComplete() called on uninitialized source");
            return;
        }

        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
            // only one commit operation must be in progress
            if (LOG.isDebugEnabled()) {
                LOG.debug("Consumer subtask {} committing offsets for checkpoint {}.",
                        getRuntimeContext().getIndexOfThisSubtask(), checkpointId);
            }

            try {
                final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
                if (posInMap == -1) {
                    LOG.warn("Consumer subtask {} received confirmation for unknown checkpoint id {}",
                            getRuntimeContext().getIndexOfThisSubtask(), checkpointId);
                    return;
                }

                @SuppressWarnings("unchecked")
                Map<PscTopicUriPartition, Long> offsets =
                        (Map<PscTopicUriPartition, Long>) pendingOffsetsToCommit.remove(posInMap);

                // remove older checkpoints in map
                for (int i = 0; i < posInMap; i++) {
                    pendingOffsetsToCommit.remove(0);
                }

                if (offsets == null || offsets.size() == 0) {
                    LOG.debug("Consumer subtask {} has empty checkpoint state.", getRuntimeContext().getIndexOfThisSubtask());
                    return;
                }

                fetcher.commitInternalOffsets(offsets, offsetCommitCallback);
            } catch (Exception e) {
                if (running) {
                    throw e;
                }
                // else ignore exception if we are no longer running
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
    }

    // ------------------------------------------------------------------------
    //  PSC Consumer specific methods
    // ------------------------------------------------------------------------

    /**
     * Creates the fetcher that connect to the backend pubsub, pulls data, deserialized the
     * data, and emits it into the data streams.
     *
     * @param sourceContext                      The source context to emit data to.
     * @param subscribedPartitionsToStartOffsets The set of partitions that this subtask should handle, with their start offsets.
     * @param watermarkStrategy                  Optional, a serialized WatermarkStrategy.
     * @param runtimeContext                     The task's runtime context.
     * @return The instantiated fetcher
     * @throws Exception The method should forward exceptions
     */
    protected abstract AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<PscTopicUriPartition, Long> subscribedPartitionsToStartOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup pubsubMetricGroup,
            boolean useMetrics) throws Exception;

    /**
     * Creates the partition discoverer that is used to find new partitions for this subtask.
     *
     * @param topicUrisDescriptor Descriptor that describes whether we are discovering partitions for fixed topics or a topic pattern.
     * @param indexOfThisSubtask  The index of this consumer subtask.
     * @param numParallelSubtasks The total number of parallel consumer subtasks.
     * @return The instantiated partition discoverer
     */
    protected abstract AbstractTopicUriPartitionDiscoverer createTopicUriPartitionDiscoverer(
            PscTopicUrisDescriptor topicUrisDescriptor,
            int indexOfThisSubtask,
            int numParallelSubtasks);

    protected abstract boolean getIsAutoCommitEnabled();

    protected abstract Map<PscTopicUriPartition, Long> fetchOffsetsWithTimestamp(
            Collection<PscTopicUriPartition> partitions,
            long timestamp);

    // ------------------------------------------------------------------------
    //  ResultTypeQueryable methods
    // ------------------------------------------------------------------------

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    // ------------------------------------------------------------------------
    //  Test utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    Map<PscTopicUriPartition, Long> getSubscribedTopicUriPartitionsToStartOffsets() {
        return subscribedTopicUriPartitionsToStartOffsets;
    }

    @VisibleForTesting
    TreeMap<PscTopicUriPartition, Long> getRestoredState() {
        return restoredState;
    }

    @VisibleForTesting
    OffsetCommitMode getOffsetCommitMode() {
        return offsetCommitMode;
    }

    @VisibleForTesting
    LinkedMap getPendingOffsetsToCommit() {
        return pendingOffsetsToCommit;
    }

    @VisibleForTesting
    public boolean getEnableCommitOnCheckpoints() {
        return enableCommitOnCheckpoints;
    }

    /**
     * Creates state serializer for PSC topic URI partition to offset tuple.
     * Using of the explicit state serializer with KryoSerializer is needed because otherwise
     * users cannot use 'disableGenericTypes' properties with PscConsumer.
     */
    @VisibleForTesting
    static TupleSerializer<Tuple2<PscTopicUriPartition, Long>> createStateSerializer(ExecutionConfig executionConfig) {
        // explicit serializer will keep the compatibility with GenericTypeInformation and allow to disableGenericTypes for users
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[]{
                new KryoSerializer<>(PscTopicUriPartition.class, executionConfig),
                LongSerializer.INSTANCE
        };
        @SuppressWarnings("unchecked")
        Class<Tuple2<PscTopicUriPartition, Long>> tupleClass = (Class<Tuple2<PscTopicUriPartition, Long>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }

}
