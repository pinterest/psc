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

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.flink.streaming.connectors.psc.testutils.TestSourceContext;
import com.pinterest.psc.common.TopicUri;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.SerializedValue;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link AbstractFetcher}.
 */
public class AbstractFetcherTest {
    private static final String TOPIC_URI =
            TopicUri.DEFAULT_PROTOCOL + ":" + TopicUri.SEPARATOR + "rn:kafka:env:cloud_region::cluster:myTopic";

    @Test
    public void testIgnorePartitionStateSentinelInSnapshot() throws Exception {
        Map<PscTopicUriPartition, Long> originalPartitions = new HashMap<>();
        originalPartitions.put(new PscTopicUriPartition(TOPIC_URI, 1), PscTopicUriPartitionStateSentinel.LATEST_OFFSET);
        originalPartitions.put(new PscTopicUriPartition(TOPIC_URI, 2), PscTopicUriPartitionStateSentinel.GROUP_OFFSET);
        originalPartitions.put(new PscTopicUriPartition(TOPIC_URI, 3), PscTopicUriPartitionStateSentinel.EARLIEST_OFFSET);

        TestSourceContext<Long> sourceContext = new TestSourceContext<>();

        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                originalPartitions,
                null, /* watermark strategy */
                new TestProcessingTimeService(),
                0);

        synchronized (sourceContext.getCheckpointLock()) {
            HashMap<PscTopicUriPartition, Long> currentState = fetcher.snapshotCurrentState();
            fetcher.commitInternalOffsets(currentState, new PscCommitCallback() {
                @Override
                public void onSuccess() {
                }

                @Override
                public void onException(Throwable cause) {
                    throw new RuntimeException("Callback failed", cause);
                }
            });

            assertThat(fetcher.getLastCommittedOffsets()).isPresent();
            assertThat(fetcher.getLastCommittedOffsets().get()).isEmpty();
        }
    }

    // ------------------------------------------------------------------------
    //   Record emitting tests
    // ------------------------------------------------------------------------

    @Test
    public void testSkipCorruptedRecord() throws Exception {
        Map<PscTopicUriPartition, Long> originalPartitions = new HashMap<>();
        PscTopicUriPartition ptup = new PscTopicUriPartition(TOPIC_URI, 1);
        originalPartitions.put(ptup, PscTopicUriPartitionStateSentinel.LATEST_OFFSET);

        TestSourceContext<Long> sourceContext = new TestSourceContext<>();

        TestFetcher<Long> fetcher = new TestFetcher<>(
                sourceContext,
                originalPartitions,
                null, /* watermark strategy */
                new TestProcessingTimeService(),
                0);

        final PscTopicUriPartitionState<Long, Object> partitionStateHolder = fetcher.subscribedPartitionStates().get(ptup);

        emitRecord(fetcher, 1L, partitionStateHolder, 1L);
        emitRecord(fetcher, 2L, partitionStateHolder, 2L);
        assertThat(sourceContext.getLatestElement().getValue().longValue()).isEqualTo(2L);
        assertThat(partitionStateHolder.getOffset()).isEqualTo(2L);

        // emit no records
        fetcher.emitRecordsWithTimestamps(emptyQueue(), partitionStateHolder, 3L, Long.MIN_VALUE);
        assertThat(sourceContext.getLatestElement().getValue().longValue())
                .isEqualTo(2L); // the null record should be skipped
        assertThat(partitionStateHolder.getOffset())
                .isEqualTo(3L); // the offset in state still should have advanced
    }

    @Test
    public void testConcurrentPartitionsDiscoveryAndLoopFetching() throws Exception {
        // test data
        final PscTopicUriPartition testPartition = new PscTopicUriPartition(TOPIC_URI, 42);

        // ----- create the test fetcher -----

        SourceContext<String> sourceContext = new TestSourceContext<>();
        Map<PscTopicUriPartition, Long> partitionsWithInitialOffsets =
                Collections.singletonMap(testPartition, PscTopicUriPartitionStateSentinel.GROUP_OFFSET);

        final OneShotLatch fetchLoopWaitLatch = new OneShotLatch();
        final OneShotLatch stateIterationBlockLatch = new OneShotLatch();

        final TestFetcher<String> fetcher = new TestFetcher<>(
                sourceContext,
                partitionsWithInitialOffsets,
                null, /* watermark strategy */
                new TestProcessingTimeService(),
                10,
                fetchLoopWaitLatch,
                stateIterationBlockLatch);

        // ----- run the fetcher -----

        final CheckedThread checkedThread = new CheckedThread() {
            @Override
            public void go() throws Exception {
                fetcher.runFetchLoop();
            }
        };
        checkedThread.start();

        // wait until state iteration begins before adding discovered partitions
        fetchLoopWaitLatch.await();
        fetcher.addDiscoveredPartitions(Collections.singletonList(testPartition));

        stateIterationBlockLatch.trigger();
        checkedThread.sync();
    }

    // ------------------------------------------------------------------------
    //  Test mocks
    // ------------------------------------------------------------------------

    private static final class TestFetcher<T> extends AbstractFetcher<T, Object> {
        Map<PscTopicUriPartition, Long> lastCommittedOffsets = null;

        private final OneShotLatch fetchLoopWaitLatch;
        private final OneShotLatch stateIterationBlockLatch;

        TestFetcher(
                SourceContext<T> sourceContext,
                Map<PscTopicUriPartition, Long> assignedPartitionsWithStartOffsets,
                SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
                ProcessingTimeService processingTimeProvider,
                long autoWatermarkInterval) throws Exception {

            this(
                    sourceContext,
                    assignedPartitionsWithStartOffsets,
                    watermarkStrategy,
                    processingTimeProvider,
                    autoWatermarkInterval,
                    null,
                    null);
        }

        TestFetcher(
                SourceContext<T> sourceContext,
                Map<PscTopicUriPartition, Long> assignedPartitionsWithStartOffsets,
                SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
                ProcessingTimeService processingTimeProvider,
                long autoWatermarkInterval,
                OneShotLatch fetchLoopWaitLatch,
                OneShotLatch stateIterationBlockLatch) throws Exception {

            super(
                    sourceContext,
                    assignedPartitionsWithStartOffsets,
                    watermarkStrategy,
                    processingTimeProvider,
                    autoWatermarkInterval,
                    TestFetcher.class.getClassLoader(),
                    new UnregisteredMetricsGroup(),
                    false);

            this.fetchLoopWaitLatch = fetchLoopWaitLatch;
            this.stateIterationBlockLatch = stateIterationBlockLatch;
        }

        /**
         * Emulation of partition's iteration which is required for
         * {@link AbstractFetcherTest#testConcurrentPartitionsDiscoveryAndLoopFetching}.
         */
        @Override
        public void runFetchLoop() throws Exception {
            if (fetchLoopWaitLatch != null) {
                for (PscTopicUriPartitionState<?, ?> ignored : subscribedPartitionStates().values()) {
                    fetchLoopWaitLatch.trigger();
                    stateIterationBlockLatch.await();
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void cancel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object createPscTopicUriPartitionHandle(PscTopicUriPartition partition) {
            return new Object();
        }

        @Override
        protected void doCommitInternalOffsets(
                Map<PscTopicUriPartition, Long> offsets,
                @Nonnull PscCommitCallback callback) {
            lastCommittedOffsets = offsets;
            callback.onSuccess();
        }

        public Optional<Map<PscTopicUriPartition, Long>> getLastCommittedOffsets() {
            return Optional.ofNullable(lastCommittedOffsets);
        }
    }

    // ------------------------------------------------------------------------

    private static <T, TUPH> void emitRecord(
            AbstractFetcher<T, TUPH> fetcher,
            T record,
            PscTopicUriPartitionState<T, TUPH> partitionState,
            long offset) {
        ArrayDeque<T> recordQueue = new ArrayDeque<>();
        recordQueue.add(record);

        fetcher.emitRecordsWithTimestamps(
                recordQueue,
                partitionState,
                offset,
                Long.MIN_VALUE);
    }

    private static <T> Queue<T> emptyQueue() {
        return new ArrayDeque<>();
    }
}
