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

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.streaming.connectors.psc.config.OffsetCommitMode;
import com.pinterest.flink.streaming.connectors.psc.internals.AbstractFetcher;
import com.pinterest.flink.streaming.connectors.psc.internals.AbstractTopicUriPartitionDiscoverer;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartitionStateSentinel;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUrisDescriptor;
import com.pinterest.flink.streaming.connectors.psc.internals.metrics.FlinkPscStateRecoveryMetricConstants;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetricsUtils;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.util.SerializedValue;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for checking whether {@link FlinkPscConsumerBase} can restore from snapshots that were
 * done using previous Flink versions' {@link FlinkPscConsumerBase}.
 *
 * Currently includes testing against 1.11 checkpoints. Additional migration support versions
 * should be added as support for Flink goes beyond version 1.11.
 *
 * <p>For regenerating the binary snapshot files run {@link #writeSnapshot()} on the corresponding
 * Flink release-* branch.
 */
@RunWith(Parameterized.class)
public class FlinkPscConsumerBaseMigrationTest {

    /**
     * Instructions: change this to the corresponding savepoint version to be written (e.g. {@link FlinkVersion#v1_3} for 1.3)
     * and remove all @Ignore annotations on writeSnapshot() methods to generate savepoints
     * Note: You should generate the savepoint based on the release branch instead of the master.
     */
    private final FlinkVersion flinkGenerateSavepointVersion = null;

    private static final HashMap<PscTopicUriPartition, Long> TOPIC_URI_PARTITION_STATE = new HashMap<>();

    static {
        TOPIC_URI_PARTITION_STATE.put(
                new PscTopicUriPartition(
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + "abc",
                        13),
                16768L
        );
        TOPIC_URI_PARTITION_STATE.put(
                new PscTopicUriPartition(
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + "def",
                        7),
                987654321L
        );
    }

    private static final List<String> TOPICS_URIS = new ArrayList<>(TOPIC_URI_PARTITION_STATE.keySet())
            .stream()
            .map(p -> p.getTopicUriStr())
            .distinct()
            .collect(Collectors.toList());

    private final FlinkVersion testMigrateVersion;

    @Parameterized.Parameters(name = "Migration Savepoint: {0}")
    public static Collection<FlinkVersion> parameters() {
        // PSC integration started with Flink 1.11
        return Arrays.asList(
                FlinkVersion.v1_11,
                FlinkVersion.v1_12,
                FlinkVersion.v1_13,
                FlinkVersion.v1_14,
                FlinkVersion.v1_15,
                FlinkVersion.v1_16
        );
    }

    public FlinkPscConsumerBaseMigrationTest(FlinkVersion testMigrateVersion) {
        this.testMigrateVersion = testMigrateVersion;
    }

    /**
     * Manually run this to write binary snapshot data.
     */
    @Ignore
    @Test
    public void writeSnapshot() throws Exception {
        writeSnapshot("src/test/resources/psc-consumer-migration-test-flink" + flinkGenerateSavepointVersion + "-snapshot", TOPIC_URI_PARTITION_STATE);

        final HashMap<PscTopicUriPartition, Long> emptyState = new HashMap<>();
        writeSnapshot("src/test/resources/psc-consumer-migration-test-flink" + flinkGenerateSavepointVersion + "-empty-state-snapshot", emptyState);
    }

    private void writeSnapshot(String path, HashMap<PscTopicUriPartition, Long> state) throws Exception {

        final OneShotLatch latch = new OneShotLatch();
        final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                latch.trigger();
                return null;
            }
        }).when(fetcher).runFetchLoop();

        when(fetcher.snapshotCurrentState()).thenReturn(state);

        final List<PscTopicUriPartition> partitions = new ArrayList<>(TOPIC_URI_PARTITION_STATE.keySet());

        final NoOpFlinkPscConsumer<String> consumerFunction =
                new NoOpFlinkPscConsumer<>(fetcher, TOPICS_URIS, partitions, FlinkPscConsumerBase.PARTITION_DISCOVERY_DISABLED);

        StreamSource<String, NoOpFlinkPscConsumer<String>> consumerOperator =
                new StreamSource<>(consumerFunction);

        final AbstractStreamOperatorTestHarness<String> testHarness =
                new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

        testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        testHarness.setup();
        testHarness.open();

        final Throwable[] error = new Throwable[1];

        // run the source asynchronously
        Thread runner = new Thread() {
            @Override
            public void run() {
                try {
                    consumerFunction.run(new NoOpSourceContext() {
                        @Override
                        public void collect(String element) {

                        }
                    });
                } catch (Throwable t) {
                    t.printStackTrace();
                    error[0] = t;
                }
            }
        };
        runner.start();

        if (!latch.isTriggered()) {
            latch.await();
        }

        final OperatorSubtaskState snapshot;
        synchronized (testHarness.getCheckpointLock()) {
            snapshot = testHarness.snapshot(0L, 0L);
        }

        OperatorSnapshotUtil.writeStateHandle(snapshot, path);

        consumerOperator.close();
        runner.join();
    }

    /**
     * Test restoring from an empty state, when no partitions could be found for topics.
     */
    @Test
    public void testRestoreFromEmptyStateNoPartitions() throws Exception {
        final NoOpFlinkPscConsumer<String> consumerFunction =
                new NoOpFlinkPscConsumer<>(
                        Collections.singletonList("noop-topic"),
                        Collections.<PscTopicUriPartition>emptyList(),
                        FlinkPscConsumerBase.PARTITION_DISCOVERY_DISABLED);

        StreamSource<String, NoOpFlinkPscConsumer<String>> consumerOperator = new StreamSource<>(consumerFunction);

        final AbstractStreamOperatorTestHarness<String> testHarness =
                new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

        testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        testHarness.setup();

        // restore state from binary snapshot file
        testHarness.initializeState(
                OperatorSnapshotUtil.getResourceFilename(
                        "psc-consumer-migration-test-flink" + testMigrateVersion + "-empty-state-snapshot"));

        testHarness.open();

        // assert that no partitions were found and is empty
        assertThat(consumerFunction.getSubscribedTopicUriPartitionsToStartOffsets()).isEmpty();

        // assert that no state was restored
        assertThat(consumerFunction.getRestoredState()).isEmpty();

        assertSuccessfulFlinkPscSourceStateMigration();

        consumerOperator.close();
        consumerOperator.cancel();
    }

    /**
     * Test restoring from an empty state taken using the current or previous Flink version, when some partitions could
     * be found for topic URIs.
     */
    @Test
    public void testRestoreFromEmptyStateWithPartitions() throws Exception {
        final List<PscTopicUriPartition> partitions = new ArrayList<>(TOPIC_URI_PARTITION_STATE.keySet());

        final NoOpFlinkPscConsumer<String> consumerFunction =
                new NoOpFlinkPscConsumer<>(TOPICS_URIS, partitions, FlinkPscConsumerBase.PARTITION_DISCOVERY_DISABLED);

        StreamSource<String, NoOpFlinkPscConsumer<String>> consumerOperator =
                new StreamSource<>(consumerFunction);

        final AbstractStreamOperatorTestHarness<String> testHarness =
                new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

        testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        testHarness.setup();

        // restore state from binary snapshot file
        testHarness.initializeState(
                OperatorSnapshotUtil.getResourceFilename(
                        "psc-consumer-migration-test-flink" + testMigrateVersion + "-empty-state-snapshot"));

        testHarness.open();

        // the expected state in "psc-consumer-migration-test-flink1.x-snapshot-empty-state";
        // all new partitions after the snapshot are considered as partitions that were created while the
        // consumer wasn't running, and should start from the earliest offset.
        final HashMap<PscTopicUriPartition, Long> expectedSubscribedPartitionsWithStartOffsets = new HashMap<>();
        for (PscTopicUriPartition partition : TOPIC_URI_PARTITION_STATE.keySet()) {
            expectedSubscribedPartitionsWithStartOffsets.put(partition, PscTopicUriPartitionStateSentinel.EARLIEST_OFFSET);
        }

        // assert that there are partitions and is identical to expected list
        assertThat(consumerFunction.getSubscribedTopicUriPartitionsToStartOffsets())
                .isNotEmpty()
                .isEqualTo(expectedSubscribedPartitionsWithStartOffsets);

        // the new partitions should have been considered as restored state
        assertThat(consumerFunction.getRestoredState()).isNotNull();
        assertThat(consumerFunction.getSubscribedTopicUriPartitionsToStartOffsets()).isNotEmpty();
        for (Map.Entry<PscTopicUriPartition, Long> expectedEntry : expectedSubscribedPartitionsWithStartOffsets.entrySet()) {
            assertThat(consumerFunction.getRestoredState())
                    .containsEntry(expectedEntry.getKey(), expectedEntry.getValue());
        }

        assertSuccessfulFlinkPscSourceStateMigration();

        consumerOperator.close();
        consumerOperator.cancel();
    }

    /**
     * Test restoring from a non-empty state taken using a previous or current Flink version, when some partitions could
     * be found for topic URIs.
     */
    @Test
    public void testRestore() throws Exception {
        final List<PscTopicUriPartition> partitions = new ArrayList<>(TOPIC_URI_PARTITION_STATE.keySet());

        final NoOpFlinkPscConsumer<String> consumerFunction =
                new NoOpFlinkPscConsumer<>(TOPICS_URIS, partitions, FlinkPscConsumerBase.PARTITION_DISCOVERY_DISABLED);

        StreamSource<String, NoOpFlinkPscConsumer<String>> consumerOperator =
                new StreamSource<>(consumerFunction);

        final AbstractStreamOperatorTestHarness<String> testHarness =
                new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

        testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        testHarness.setup();

        // restore state from binary snapshot file
        testHarness.initializeState(
                OperatorSnapshotUtil.getResourceFilename(
                        "psc-consumer-migration-test-flink" + testMigrateVersion + "-snapshot"));

        testHarness.open();

        // assert that there are partitions and is identical to expected list
        assertThat(consumerFunction.getSubscribedTopicUriPartitionsToStartOffsets())
                .isNotEmpty()
                // on restore, subscribedPartitionsToStartOffsets should be identical to the
                // restored state
                .isEqualTo(TOPIC_URI_PARTITION_STATE);

        // assert that state is correctly restored from legacy checkpoint
        assertThat(consumerFunction.getRestoredState()).isNotNull().isEqualTo(TOPIC_URI_PARTITION_STATE);

        assertSuccessfulFlinkPscSourceStateMigration();

        consumerOperator.close();
        consumerOperator.cancel();
    }

    private void assertSuccessfulFlinkPscSourceStateMigration() {
        assertThat(
                PscMetricRegistryManager.getInstance().getCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_SUCCESS, null
                )).isGreaterThan(0);

        assertThat(
                PscMetricRegistryManager.getInstance().getCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_PSC_FAILURE, null
                )).isEqualTo(0);

        assertThat(
                PscMetricRegistryManager.getInstance().getCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_SUCCESS, null
                )).isEqualTo(0);

        assertThat(
                PscMetricRegistryManager.getInstance().getCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_RECOVERY_KAFKA_FAILURE, null
                )).isEqualTo(0);

        assertThat(
                PscMetricRegistryManager.getInstance().getCounterMetric(
                        null, FlinkPscStateRecoveryMetricConstants.PSC_SOURCE_STATE_FRESH, null
                )).isEqualTo(0);
    }

    // ------------------------------------------------------------------------

    private static class NoOpFlinkPscConsumer<T> extends FlinkPscConsumerBase<T> {
        private static final long serialVersionUID = 1L;

        private final List<PscTopicUriPartition> partitions;

        private final AbstractFetcher<T, ?> fetcher;

        @SuppressWarnings("unchecked")
        NoOpFlinkPscConsumer(
                AbstractFetcher<T, ?> fetcher,
                List<String> topics,
                List<PscTopicUriPartition> partitions,
                long discoveryInterval) {

            super(
                    topics,
                    null,
                    (PscDeserializationSchema<T>) mock(PscDeserializationSchema.class),
                    discoveryInterval,
                    false,
                    PscConfigurationUtils.pscConfigurationInternalToProperties(PscMetricsUtils.initializePscMetrics(true))
            );

            this.fetcher = fetcher;
            this.partitions = partitions;

        }

        NoOpFlinkPscConsumer(List<String> topics, List<PscTopicUriPartition> partitions, long discoveryInterval) {
            this(mock(AbstractFetcher.class), topics, partitions, discoveryInterval);
        }

        @Override
        public void close() throws Exception {
            PscMetricsUtils.shutdownPscMetrics();
        }

        @Override
        protected AbstractFetcher<T, ?> createFetcher(
                SourceContext<T> sourceContext,
                Map<PscTopicUriPartition, Long> thisSubtaskPartitionsWithStartOffsets,
                SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
                StreamingRuntimeContext runtimeContext,
                OffsetCommitMode offsetCommitMode,
                MetricGroup consumerMetricGroup,
                boolean useMetrics) throws Exception {
            return fetcher;
        }

        @Override
        protected AbstractTopicUriPartitionDiscoverer createTopicUriPartitionDiscoverer(
                PscTopicUrisDescriptor topicsDescriptor,
                int indexOfThisSubtask,
                int numParallelSubtasks) {

            AbstractTopicUriPartitionDiscoverer mockPartitionDiscoverer = mock(AbstractTopicUriPartitionDiscoverer.class);

            try {
                when(mockPartitionDiscoverer.discoverPartitions()).thenReturn(partitions);
            } catch (Exception e) {
                // ignore
            }
            when(mockPartitionDiscoverer.setAndCheckDiscoveredPartition(any(PscTopicUriPartition.class))).thenReturn(true);

            return mockPartitionDiscoverer;
        }

        @Override
        protected boolean getIsAutoCommitEnabled() {
            return false;
        }

        @Override
        protected Map<PscTopicUriPartition, Long> fetchOffsetsWithTimestamp(
                Collection<PscTopicUriPartition> partitions,
                long timestamp) {
            throw new UnsupportedOperationException();
        }
    }

    private abstract static class NoOpSourceContext
            implements SourceFunction.SourceContext<String> {

        private final Object lock = new Object();

        @Override
        public void collectWithTimestamp(String element, long timestamp) {
        }

        @Override
        public void emitWatermark(Watermark mark) {
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }

        @Override
        public void close() {
        }

        @Override
        public void markAsTemporarilyIdle() {

        }
    }
}
