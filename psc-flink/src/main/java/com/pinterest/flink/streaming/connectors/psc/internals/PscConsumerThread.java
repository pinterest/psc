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

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.event.PscEvent;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.OffsetCommitCallback;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.interceptor.TypePreservingInterceptor;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import java.io.IOException;
import java.util.Arrays;
import java.util.Queue;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import com.pinterest.flink.streaming.connectors.psc.internals.metrics.PscMetricWrapper;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The thread the runs the {@link PscConsumer}, connecting to the brokers and
 * polling records. The thread pushes the data into a {@link Handover} to be
 * picked up by the fetcher that will deserialize and emit the records.
 *
 * <p>
 * <b>IMPORTANT:</b> This thread must not be interrupted when attempting to shut
 * it down. The PSC consumer code was found to not always handle interrupts
 * well, and to even deadlock in certain situations.
 *
 * <p>
 * Implementation Note: This code is written to be reusable in later versions of
 * the PscConsumer. Because the pubsub may not be maintaining binary compatibility, we
 * use a "call bridge" as an indirection to the PscConsumer calls that change
 * signature.
 */
@Internal
public class PscConsumerThread<T> extends Thread {

    /**
     * Logger for this consumer.
     */
    private final Logger log;

    /**
     * The handover of data and exceptions between the consumer thread and the task
     * thread.
     */
    private final Handover handover;

    /**
     * The next offsets that the main thread should commit and the commit callback.
     */
    private final AtomicReference<Tuple2<Map<TopicUriPartition, Long>, PscCommitCallback>> nextOffsetsToCommit;

    /**
     * The configuration for the PSC consumer.
     */
    private final Properties pscProperties;

    /**
     * The queue of unassigned partitions that we need to assign to the PSC
     * consumer.
     */
    private final ClosableBlockingQueue<PscTopicUriPartitionState<T, TopicUriPartition>> unassignedTopicUriPartitionsQueue;

    /**
     * The maximum number of milliseconds to wait for a fetch batch.
     */
    private final long pollTimeout;

    /**
     * Flag whether to add PSC's metrics to the Flink metrics.
     */
    private final boolean useMetrics;

    /**
     * @deprecated We should only be publishing to the
     * {{@link #consumerMetricGroup}}. This is kept to retain
     * compatibility for metrics.
     **/
    @Deprecated
    private final MetricGroup subtaskMetricGroup;

    /**
     * We get this from the outside to publish metrics.
     */
    private final MetricGroup consumerMetricGroup;

    /**
     * Reference to the PSC consumer, once it is created.
     */
    private volatile PscConsumer<byte[], byte[]> consumer;

    /**
     * This lock is used to isolate the consumer for partition reassignment.
     */
    private final Object consumerReassignmentLock;

    /**
     * Indication if this consumer has any assigned partition.
     */
    private boolean hasAssignedPartitions;

    /**
     * Flag to indicate whether an external operation ({@link #setOffsetsToCommit(Map, PscCommitCallback)} or
     * {@link #shutdown()}) had attempted to wakeup the consumer while it was isolated for partition reassignment.
     */
    private volatile boolean hasBufferedWakeup;

    /**
     * Flag to mark the main work loop as alive.
     */
    private volatile boolean running;

    /**
     * Flag tracking whether the latest commit request has completed.
     */
    private volatile boolean commitInProgress;

    private boolean hasInitializedMetrics;

    private transient AtomicBoolean pscMetricsInitialized;

    private transient PscConfigurationInternal pscConfigurationInternal;

    public PscConsumerThread(Logger log,
                             Handover handover,
                             Properties pscProperties,
                             ClosableBlockingQueue<PscTopicUriPartitionState<T, TopicUriPartition>> unassignedTopicUriPartitionsQueue,
                             String threadName,
                             long pollTimeout,
                             boolean useMetrics,
                             MetricGroup consumerMetricGroup,
                             MetricGroup subtaskMetricGroup) {

        super(threadName);
        setDaemon(true);

        this.log = checkNotNull(log);
        this.handover = checkNotNull(handover);
        this.pscProperties = checkNotNull(pscProperties);
        this.consumerMetricGroup = checkNotNull(consumerMetricGroup);
        this.subtaskMetricGroup = checkNotNull(subtaskMetricGroup);

        this.unassignedTopicUriPartitionsQueue = checkNotNull(unassignedTopicUriPartitionsQueue);

        this.pollTimeout = pollTimeout;
        this.useMetrics = useMetrics;

        this.consumerReassignmentLock = new Object();
        this.nextOffsetsToCommit = new AtomicReference<>();
        this.running = true;
        this.hasInitializedMetrics = false;
        this.pscConfigurationInternal = PscConfigurationUtils.propertiesToPscConfigurationInternal(pscProperties, PscConfigurationInternal.PSC_CLIENT_TYPE_CONSUMER);

        initializeInterceptor();
    }

    // ------------------------------------------------------------------------

    @Override
    public void run() {
        // early exit check
        if (!running) {
            return;
        }

        // this is the means to talk to FlinkPscConsumer's main thread
        final Handover handover = this.handover;

        // This method initializes the PscConsumer and guarantees it is torn down properly.
        // This is important, because the consumer has multi-threading issues,
        // including concurrent 'close()' calls.
        try {
            this.consumer = getConsumer(pscProperties);
        } catch (Throwable t) {
            handover.reportError(t);
            return;
        }

        // The latest bulk of records. May carry across the loop if the thread is woken up
        // from blocking on the handover
        PscConsumerPollMessageIterator<byte[], byte[]> records = null;

        // from here on, the consumer is guaranteed to be closed properly
        try {
            // early exit check
            if (!running) {
                return;
            }

            // reused variable to hold found unassigned new partitions.
            // found partitions are not carried across loops using this variable;
            // they are carried across via re-adding them to the unassigned partitions queue
            List<PscTopicUriPartitionState<T, TopicUriPartition>> newPartitions;

            // main fetch loop
            while (running) {

                // check if there is something to commit
                if (!commitInProgress) {
                    // get and reset the work-to-be committed, so we don't repeatedly commit the
                    // same
                    final Tuple2<Map<TopicUriPartition, Long>, PscCommitCallback> commitOffsetsAndCallback =
                            nextOffsetsToCommit.getAndSet(null);

                    if (commitOffsetsAndCallback != null && !commitOffsetsAndCallback.f0.isEmpty()) {
                        log.debug("Sending async offset commit request to backend pubsub");

                        // also record that a commit is already in progress
                        // the order here matters! first set the flag, then send the commit command.
                        commitInProgress = true;
                        List<MessageId> messageIds = commitOffsetsAndCallback.f0.entrySet().stream().map(
                                e -> new MessageId(e.getKey(), e.getValue())
                        ).collect(Collectors.toList());
                        consumer.commitAsync(messageIds, new CommitCallback(commitOffsetsAndCallback.f1));
                    }
                }

                try {
                    if (hasAssignedPartitions) {
                        newPartitions = unassignedTopicUriPartitionsQueue.pollBatch();
                    } else {
                        // if no assigned partitions block until we get at least one
                        // instead of hot spinning this loop. We rely on a fact that
                        // unassignedPartitionsQueue will be closed on a shutdown, so
                        // we don't block indefinitely
                        newPartitions = unassignedTopicUriPartitionsQueue.getBatchBlocking();
                    }
                    if (newPartitions != null) {
                        reassignPartitions(newPartitions);
                    }
                } catch (AbortedReassignmentException e) {
                    continue;
                }

                if (!hasAssignedPartitions) {
                    // Without assigned partitions PscConsumer.poll will throw an exception
                    continue;
                }

                // get the next batch of records, unless we did not manage to hand the old batch
                // over
                if (records == null) {
                    try {
                        records = consumer.poll(Duration.ofMillis(pollTimeout));
                        if (!hasInitializedMetrics) {
                            initializeMetrics();
                            hasInitializedMetrics = true;
                        }
                    } catch (WakeupException we) {
                        continue;
                    }
                }

                try {
                    handover.produce(records);
                    records = null;
                } catch (Handover.WakeupException e) {
                    Queue<PscEvent> events = handover.getEventQueue();
                    while (!events.isEmpty()) {
                        consumer.onEvent(events.poll());
                    }
                    // fall through the loop
                }
            }
            // end main fetch loop
        } catch (Throwable t) {
            // let the main thread know and exit
            // it may be that this exception comes because the main thread closed the
            // handover, in
            // which case the below reporting is irrelevant, but does not hurt either
            handover.reportError(t);
        } finally {
            // make sure the handover is closed if it is not already closed or has an error
            handover.close();

            if (records != null) {
                try {
                    records.close();
                } catch (IOException ioe) {
                    // pass through; best effort close
                }
            }

            // make sure the PscConsumer is closed
            try {
                consumer.close();
            } catch (Throwable t) {
                log.warn("Error while closing PSC consumer", t);
            }
        }
    }

    private void initializeMetrics() throws ClientException {
        // initialize Psc metrics
        initializePscMetrics();
        // register PSC's very own metrics in Flink's metric reporters
        if (useMetrics) {
            // register PSC metrics to Flink
            Map<MetricName, ? extends Metric> metrics = consumer.metrics();
            if (metrics == null) {
              // MapR's PSC implementation returns null here.
              log.info("Consumer implementation does not support metrics");
            } else {
              // we have PSC metrics, register them
              for (Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
                if (
                        !(metric.getValue().metricValue() instanceof Double) ||
                        (metric.getKey().group() != null && metric.getKey().group().equals("consumer-node-metrics"))
                )
                    continue;

                consumerMetricGroup.gauge(metric.getKey().name(), new PscMetricWrapper(metric.getValue()));

                // TODO this metric is kept for compatibility purposes; should remove in the future
                subtaskMetricGroup.gauge(metric.getKey().name(), new PscMetricWrapper(metric.getValue()));
              }
            }
        }
    }

    private void initializeInterceptor() {
        TypePreservingInterceptor<byte[], byte[]> eventInterceptor = new PscFlinkConsumerEventInterceptor<>(handover);
        Object interceptors = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES);
        if (interceptors == null) {
            pscProperties.put(PscConfiguration.PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES, eventInterceptor);
        } else {
            pscProperties.put(PscConfiguration.PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES, Arrays.asList(interceptors, eventInterceptor));
        }
    }

    private void initializePscMetrics() {
        log.info("Initializing PSC metrics in PscConsumerThread from threadId {}", Thread.currentThread().getId());
        if (pscMetricsInitialized == null)
            pscMetricsInitialized = new AtomicBoolean(false);

        if (pscMetricsInitialized.compareAndSet(false, true)) {
            PscMetricRegistryManager.getInstance().initialize(pscConfigurationInternal);
        }
    }

    private void shutdownPscMetrics() {
        if (pscMetricsInitialized != null && pscMetricsInitialized.compareAndSet(true, false))
            PscMetricRegistryManager.getInstance().shutdown(pscConfigurationInternal);
    }

    /**
     * Shuts this thread down, waking up the thread gracefully if blocked (without
     * Thread.interrupt() calls).
     */
    public void shutdown() {
        running = false;

        // wake up all blocking calls on the queue
        unassignedTopicUriPartitionsQueue.close();

        // We cannot call close() on the PscConsumer, because it will actually throw
        // an exception if a concurrent call is in progress

        // this wakes up the consumer if it is blocked handing over records
        handover.wakeupProducer();

        shutdownPscMetrics();

        // this wakes up the consumer if it is blocked in a PSC poll
        synchronized (consumerReassignmentLock) {
            if (consumer != null) {
                consumer.wakeup();
            } else {
                // the consumer is currently isolated for partition reassignment;
                // set this flag so that the wakeup state is restored once the reassignment is
                // complete
                hasBufferedWakeup = true;
            }
        }
    }

    /**
     * Tells this thread to commit a set of offsets. This method does not block, the
     * committing operation will happen asynchronously.
     *
     * <p>
     * Only one commit operation may be pending at any time. If the committing takes
     * longer than the frequency with which this method is called, then some commits
     * may be skipped due to being superseded by newer ones.
     *
     * @param offsetsToCommit The offsets to commit
     * @param commitCallback  callback when PSC commit completes
     */
    void setOffsetsToCommit(Map<TopicUriPartition, Long> offsetsToCommit,
                            @Nonnull PscCommitCallback commitCallback) {

        // record the work to be committed by the main consumer thread and make sure the
        // consumer notices that
        if (nextOffsetsToCommit.getAndSet(Tuple2.of(offsetsToCommit, commitCallback)) != null) {
            log.warn("Committing offsets takes longer than the checkpoint interval. "
                    + "Skipping commit of previous offsets because newer complete checkpoint offsets are available. "
                    + "This does not compromise Flink's checkpoint integrity.");
        }

        // if the consumer is blocked in a poll() or handover operation, wake it up to
        // commit soon
        handover.wakeupProducer();

        synchronized (consumerReassignmentLock) {
            if (consumer != null) {
                consumer.wakeup();
            } else {
                // the consumer is currently isolated for partition reassignment;
                // set this flag so that the wakeup state is restored once the reassignment is
                // complete
                hasBufferedWakeup = true;
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Reestablishes the assigned partitions for the consumer. The reassigned
     * partitions consists of the provided new partitions and whatever partitions
     * was already previously assigned to the consumer.
     *
     * <p>
     * The reassignment process is protected against wakeup calls, so that after
     * this method returns, the consumer is either untouched or completely
     * reassigned with the correct offset positions.
     *
     * <p>
     * If the consumer was already woken-up prior to a reassignment resulting in an
     * interruption any time during the reassignment, the consumer is guaranteed to
     * roll back as if it was untouched. On the other hand, if there was an attempt
     * to wakeup the consumer during the reassignment, the wakeup call is "buffered"
     * until the reassignment completes.
     *
     * <p>
     * This method is exposed for testing purposes.
     */
    @VisibleForTesting
    void reassignPartitions(List<PscTopicUriPartitionState<T, TopicUriPartition>> newPartitions) throws Exception {
        if (newPartitions.size() == 0) {
            return;
        }
        hasAssignedPartitions = true;
        boolean reassignmentStarted = false;

        // since the reassignment may introduce several PSC blocking calls that cannot
        // be interrupted,
        // the consumer needs to be isolated from external wakeup calls in
        // setOffsetsToCommit() and shutdown()
        // until the reassignment is complete.
        final PscConsumer<byte[], byte[]> consumerTmp;
        synchronized (consumerReassignmentLock) {
            consumerTmp = this.consumer;
            this.consumer = null;
        }

        final Map<TopicUriPartition, Long> oldPartitionAssignmentsToPosition = new HashMap<>();
        try {
            for (TopicUriPartition oldPartition : consumerTmp.assignment()) {
                oldPartitionAssignmentsToPosition.put(oldPartition, consumerTmp.position(oldPartition));
            }

            final List<TopicUriPartition> newPartitionAssignments = new ArrayList<>(
                    newPartitions.size() + oldPartitionAssignmentsToPosition.size());
            newPartitionAssignments.addAll(oldPartitionAssignmentsToPosition.keySet());
            newPartitionAssignments.addAll(convertPscTopicUriPartitions(newPartitions));

            // reassign with the new partitions
            consumerTmp.assign(newPartitionAssignments);
            reassignmentStarted = true;

            // old partitions should be seeked to their previous position
            for (Entry<TopicUriPartition, Long> oldPartitionToPosition : oldPartitionAssignmentsToPosition
                    .entrySet()) {
                consumerTmp.seekToOffset(oldPartitionToPosition.getKey(), oldPartitionToPosition.getValue());
            }

            // offsets in the state of new partitions may still be placeholder sentinel
            // values if we are:
            // (1) starting fresh,
            // (2) checkpoint / savepoint state we were restored with had not completely
            // been replaced with actual offset values yet, or
            // (3) the partition was newly discovered after startup;
            // replace those with actual offsets, according to what the sentinel value
            // represent.
            for (PscTopicUriPartitionState<T, TopicUriPartition> newPartitionState : newPartitions) {
                if (newPartitionState.getOffset() == PscTopicUriPartitionStateSentinel.EARLIEST_OFFSET) {
                    consumerTmp.seekToBeginning(
                            Collections.singletonList(newPartitionState.getPscTopicUriPartitionHandle()));
                    newPartitionState
                            .setOffset(consumerTmp.position(newPartitionState.getPscTopicUriPartitionHandle()) - 1);
                } else if (newPartitionState
                        .getOffset() == PscTopicUriPartitionStateSentinel.LATEST_OFFSET) {
                    consumerTmp
                            .seekToEnd(Collections.singletonList(newPartitionState.getPscTopicUriPartitionHandle()));
                    newPartitionState
                            .setOffset(consumerTmp.position(newPartitionState.getPscTopicUriPartitionHandle()) - 1);
                } else if (newPartitionState.getOffset() == PscTopicUriPartitionStateSentinel.GROUP_OFFSET) {
                    // the PscConsumer by default will automatically seek the consumer position
                    // to the committed group offset, so we do not need to do it.

                    newPartitionState
                            .setOffset(consumerTmp.position(newPartitionState.getPscTopicUriPartitionHandle()) - 1);
                } else {
                    consumerTmp.seekToOffset(
                            newPartitionState.getPscTopicUriPartitionHandle(), newPartitionState.getOffset() + 1
                    );
                }
            }
        } catch (WakeupException e) {
            // a WakeupException may be thrown if the consumer was invoked wakeup()
            // before it was isolated for the reassignment. In this case, we abort the
            // reassignment and just re-expose the original consumer.

            synchronized (consumerReassignmentLock) {
                this.consumer = consumerTmp;

                // if reassignment had already started and affected the consumer,
                // we do a full roll back so that it is as if it was left untouched
                if (reassignmentStarted) {
                    this.consumer.assign(new ArrayList<>(oldPartitionAssignmentsToPosition.keySet()));

                    for (Entry<TopicUriPartition, Long> oldPartitionToPosition : oldPartitionAssignmentsToPosition
                            .entrySet()) {
                        this.consumer.seekToOffset(oldPartitionToPosition.getKey(), oldPartitionToPosition.getValue());
                    }
                }

                // no need to restore the wakeup state in this case,
                // since only the last wakeup call is effective anyways
                hasBufferedWakeup = false;

                // re-add all new partitions back to the unassigned partitions queue to be
                // picked up again
                for (PscTopicUriPartitionState<T, TopicUriPartition> newPartition : newPartitions) {
                    unassignedTopicUriPartitionsQueue.add(newPartition);
                }

                // this signals the main fetch loop to continue through the loop
                throw new AbortedReassignmentException();
            }
        }

        // reassignment complete; expose the reassigned consumer
        synchronized (consumerReassignmentLock) {
            this.consumer = consumerTmp;

            // restore wakeup state for the consumer if necessary
            if (hasBufferedWakeup) {
                this.consumer.wakeup();
                hasBufferedWakeup = false;
            }
        }
    }

    @VisibleForTesting
    public static PscConsumer<byte[], byte[]> getConsumer(Properties pscConsumerProperties) throws ConfigurationException,
            ConsumerException {

        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConsumerProperties.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value));
        return new PscConsumer<>(pscConfiguration);
    }

    // ------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------

    private static <T> List<TopicUriPartition> convertPscTopicUriPartitions(List<PscTopicUriPartitionState<T, TopicUriPartition>> partitions) {
        ArrayList<TopicUriPartition> result = new ArrayList<>(partitions.size());
        for (PscTopicUriPartitionState<T, TopicUriPartition> p : partitions) {
            result.add(p.getPscTopicUriPartitionHandle());
        }
        return result;
    }

    private class CommitCallback implements OffsetCommitCallback {
        private final PscCommitCallback internalCommitCallback;

        CommitCallback(PscCommitCallback internalCommitCallback) {
            this.internalCommitCallback = checkNotNull(internalCommitCallback);
        }

        @Override
        public void onCompletion(Map<TopicUriPartition, MessageId> offsets, Exception ex) {
            commitInProgress = false;

            if (ex != null) {
                log.warn("Committing offsets to PSC failed. This does not compromise Flink's checkpoints.", ex);
                internalCommitCallback.onException(ex);
            } else {
                internalCommitCallback.onSuccess();
            }
        }
    }

    /**
     * Utility exception that serves as a signal for the main loop to continue
     * through the loop if a reassignment attempt was aborted due to an
     * pre-reassignment wakeup call on the consumer.
     */
    private static class AbortedReassignmentException extends Exception {
        private static final long serialVersionUID = 1L;
    }
}
