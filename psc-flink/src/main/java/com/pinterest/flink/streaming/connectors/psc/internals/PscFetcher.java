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

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import java.util.Collection;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.pinterest.flink.streaming.connectors.psc.PscDeserializationSchema;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A fetcher that fetches data from backend pubsub via the PSC consumer API.
 *
 * @param <T> The type of elements produced by the fetcher.
 */
@Internal
public class PscFetcher<T> extends AbstractFetcher<T, TopicUriPartition> {

    private static final Logger LOG = LoggerFactory.getLogger(PscFetcher.class);

    // ------------------------------------------------------------------------

    /**
     * The schema to convert between PSC's byte messages, and Flink's objects.
     */
    private final PscDeserializationSchema<T> deserializer;

    /**
     * A collector to emit records in batch (bundle).
     **/
    private final PscCollector pscCollector;

    /**
     * The handover of data and exceptions between the consumer thread and the task thread.
     */
    final Handover handover;

    /**
     * The thread that runs the actual PscConsumer and hand the record batches to this fetcher.
     */
    final PscConsumerThread<T> consumerThread;

    /**
     * Flag to mark the main work loop as alive.
     */
    volatile boolean running = true;

    // ------------------------------------------------------------------------

    public PscFetcher(
            SourceFunction.SourceContext<T> sourceContext,
            Map<PscTopicUriPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            String taskNameWithSubtasks,
            PscDeserializationSchema<T> deserializer,
            Properties pscConsumerProperties,
            long pollTimeout,
            MetricGroup subtaskMetricGroup,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {
        super(
                sourceContext,
                assignedPartitionsWithInitialOffsets,
                watermarkStrategy,
                processingTimeProvider,
                autoWatermarkInterval,
                userCodeClassLoader,
                consumerMetricGroup,
                useMetrics);

        this.deserializer = deserializer;
        this.handover = new Handover();

        this.consumerThread = new PscConsumerThread<T>(
                LOG,
                handover,
                pscConsumerProperties,
                unassignedTopicUriPartitionsQueue,
                getFetcherName() + " for " + taskNameWithSubtasks,
                pollTimeout,
                useMetrics,
                consumerMetricGroup,
                subtaskMetricGroup
        );
        this.pscCollector = new PscCollector();
    }

    // ------------------------------------------------------------------------
    //  Fetcher work methods
    // ------------------------------------------------------------------------

    @Override
    public void runFetchLoop() throws Exception {
        try {
            // kick off the actual PSC consumer
            consumerThread.start();

            while (running) {
                // this blocks until we get the next records
                // it automatically re-throws exceptions encountered in the consumer thread

                try (PscConsumerPollMessageIterator<byte[], byte[]> records = handover.pollNext()) {
                    while (records.hasNext()) {
                        PscConsumerMessage<byte[], byte[]> record = records.next();
                        PscTopicUriPartition
                            key =
                            new PscTopicUriPartition(
                                record.getMessageId().getTopicUriPartition().getTopicUri(),
                                record.getMessageId().getTopicUriPartition().getPartition());
                        PscTopicUriPartitionState<T, TopicUriPartition>
                            partitionState =
                            subscribedPartitionStates().get(key);
                        if (partitionState != null) {
                            topicUriPartitionConsumerRecordsHandler(record, partitionState);
                        }
                    }
                }

                // retired code; doesn't work for memq due to no support of `iteratorFor` method
                // get the records for each topic partition
                /* for (PscTopicUriPartitionState<T, TopicUriPartition> partition : subscribedPartitionStates()) {

                    Iterator<PscConsumerMessage<byte[], byte[]>> partitionRecords =
                            records.iteratorFor(partition.getPscTopicUriPartitionHandle());

                    topicUriPartitionConsumerRecordsHandler(partitionRecords, partition);
                }*/
            }
        } finally {
            // this signals the consumer thread that no more work is to be done
            consumerThread.shutdown();
        }

        // on a clean exit, wait for the runner thread
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            // may be the result of a wake-up interruption after an exception.
            // we ignore this here and only restore the interruption state
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void cancel() {
        // flag the main thread to exit. A thread interrupt will come anyways.
        running = false;
        handover.close();
        consumerThread.shutdown();
    }

    /**
     * Gets the name of this fetcher, for thread naming and logging purposes.
     */
    protected String getFetcherName() {
        return "PSC Fetcher";
    }

    protected void topicUriPartitionConsumerRecordsHandler(
            PscConsumerMessage<byte[], byte[]> record,
            PscTopicUriPartitionState<T, TopicUriPartition> pscTopicUriPartitionState) throws Exception {

//        while (topicUriPartitionMessages.hasNext()) {
//            PscConsumerMessage<byte[], byte[]> record = topicUriPartitionMessages.next();
            deserializer.deserialize(record, pscCollector);

            // emit the actual records. this also updates offset state atomically and emits
            // watermarks
            emitRecordsWithTimestamps(
                    pscCollector.getRecords(),
                    pscTopicUriPartitionState,
                    record.getMessageId().getOffset(),
                    record.getPublishTimestamp());

            if (pscCollector.isEndOfStreamSignalled()) {
                // end of stream signaled
                running = false;
//                break;
            }
//        }
    }

    // ------------------------------------------------------------------------
    //  Implement Methods of the AbstractFetcher
    // ------------------------------------------------------------------------

    @Override
    public TopicUriPartition createPscTopicUriPartitionHandle(PscTopicUriPartition pscTopicUriPartition) {
        return new TopicUriPartition(pscTopicUriPartition.getTopicUriStr(), pscTopicUriPartition.getPartition());
    }

    @Override
    protected void doCommitInternalOffsets(
            Map<PscTopicUriPartition, Long> topicUriPartitionOffsets,
            @Nonnull PscCommitCallback commitCallback) throws Exception {

        Collection<PscTopicUriPartitionState<T, TopicUriPartition>> pscTopicUriPartitionStates = subscribedPartitionStates().values();

        Map<TopicUriPartition, Long> offsetsToCommit = new HashMap<>(pscTopicUriPartitionStates.size());

        for (PscTopicUriPartitionState<T, TopicUriPartition> pscTopicUriPartitionState : pscTopicUriPartitionStates) {
            Long lastProcessedOffset = topicUriPartitionOffsets.get(pscTopicUriPartitionState.getPscTopicUriPartition());
            if (lastProcessedOffset != null) {
                checkState(!PscTopicUriPartitionStateSentinel.isSentinel(lastProcessedOffset), "Illegal offset value to commit");

                // committed offsets through the PscConsumer need to be the last processed offset.
                // PSC will translate that to the proper offset to commit per backend.
                // This does not affect Flink's checkpoints/saved state.
                long offsetToCommit = lastProcessedOffset;

                offsetsToCommit.put(pscTopicUriPartitionState.getPscTopicUriPartitionHandle(), offsetToCommit);
                pscTopicUriPartitionState.setCommittedOffset(offsetToCommit);
            }
        }

        // record the work to be committed by the main consumer thread and make sure the consumer notices that
        consumerThread.setOffsetsToCommit(offsetsToCommit, commitCallback);
    }

    private class PscCollector implements Collector<T> {
        private final Queue<T> records = new ArrayDeque<>();

        private boolean endOfStreamSignalled = false;

        @Override
        public void collect(T record) {
            // do not emit subsequent elements if the end of the stream reached
            if (endOfStreamSignalled || deserializer.isEndOfStream(record)) {
                endOfStreamSignalled = true;
                return;
            }
            records.add(record);
        }

        public Queue<T> getRecords() {
            return records;
        }

        public boolean isEndOfStreamSignalled() {
            return endOfStreamSignalled;
        }

        @Override
        public void close() {

        }
    }
}
