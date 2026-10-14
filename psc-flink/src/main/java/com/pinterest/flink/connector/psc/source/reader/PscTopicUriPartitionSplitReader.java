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

package com.pinterest.flink.connector.psc.source.reader;

import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.OffsetCommitCallback;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** A {@link SplitReader} implementation that reads records from PSC TopicUriPartitions. */
@Internal
public class PscTopicUriPartitionSplitReader
        implements SplitReader<PscConsumerMessage<byte[], byte[]>, PscTopicUriPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PscTopicUriPartitionSplitReader.class);
    private static final long POLL_TIMEOUT = 10000L;
    private static final double MIN_SUBTASK_RATE_LIMIT_QPS = 0.1;
    private static final int DEFAULT_POLL_MESSAGES_MAX = 500;

    private final PscConsumer<byte[], byte[]> consumer;
    private final Map<TopicUriPartition, Long> stoppingOffsets;
    private final String groupId;
    private final int subtaskId;

    private final PscSourceReaderMetrics pscSourceReaderMetrics;

    // Tracking empty splits that has not been added to finished splits in fetch()
    private final Set<String> emptySplits = new HashSet<>();
    private final Properties props;

    /** Optional fetch-side rate limiter; null when scan.rate-limit is unset. */
    @Nullable private final RateLimiter fetchRateLimiter;

    /**
     * Permits to acquire before the next {@link #fetch()}. Starts at poll.messages.max so the first
     * parallel startup fetches are staggered; then tracks the previous poll's emitted count.
     */
    private int nextFetchRatePermits;

    /** Partitions finished while streaming the previous poll; unassigned at the start of fetch(). */
    private final List<TopicUriPartition> pendingUnassignPartitions = new ArrayList<>();

    public PscTopicUriPartitionSplitReader(
            Properties props,
            SourceReaderContext context,
            PscSourceReaderMetrics pscSourceReaderMetrics) throws ConfigurationException, ClientException {
        this(props, context, pscSourceReaderMetrics, null);
    }

    public PscTopicUriPartitionSplitReader(
            Properties props,
            SourceReaderContext context,
            PscSourceReaderMetrics pscSourceReaderMetrics,
            String rackIdSupplier) throws ConfigurationException, ConsumerException {
        this.props = props;
        this.subtaskId = context.getIndexOfSubtask();
        this.pscSourceReaderMetrics = pscSourceReaderMetrics;
        Properties consumerProps = new Properties();
        consumerProps.putAll(props);
        consumerProps.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, createConsumerClientId(props));
        setConsumerClientRack(consumerProps, rackIdSupplier);
        this.consumer = new PscConsumer<>(PscConfigurationUtils.propertiesToPscConfiguration(consumerProps));
        this.stoppingOffsets = new HashMap<>();
        this.groupId = consumerProps.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        int pollMessagesMax =
                parsePositiveInt(
                        props.getProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX),
                        DEFAULT_POLL_MESSAGES_MAX);
        this.nextFetchRatePermits = pollMessagesMax;
        this.fetchRateLimiter = createFetchRateLimiter(props, context.currentParallelism(), pollMessagesMax);
    }

    @Nullable
    private static RateLimiter createFetchRateLimiter(
            Properties props, int parallelism, int pollMessagesMax) {
        String rateLimitStr =
                props.getProperty(PscSourceOptions.SCAN_RATE_LIMIT_RECORDS_PER_SECOND.key());
        if (rateLimitStr == null || rateLimitStr.isEmpty()) {
            return null;
        }
        double totalRate = Double.parseDouble(rateLimitStr);
        if (totalRate <= 0) {
            return null;
        }
        int parallel = Math.max(1, parallelism);
        double subtaskRate = totalRate / parallel;
        Preconditions.checkArgument(
                subtaskRate > MIN_SUBTASK_RATE_LIMIT_QPS,
                "Subtask rate limit should be greater than %s QPS. "
                        + "Current rate: %s records/second divided by %s subtasks = %s records/second per subtask. "
                        + "Consider increasing the rate limit or decreasing parallelism.",
                MIN_SUBTASK_RATE_LIMIT_QPS,
                totalRate,
                parallel,
                subtaskRate);
        LOG.info(
                "Fetch-side rate limit enabled: {} records/second total, {}/s per subtask "
                        + "(parallelism={}, initial batch permits={})",
                totalRate,
                subtaskRate,
                parallel,
                pollMessagesMax);
        return RateLimiter.create(subtaskRate);
    }

    private static int parsePositiveInt(@Nullable String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value);
            return parsed > 0 ? parsed : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private void acquireFetchRatePermitsBeforePoll() {
        if (fetchRateLimiter == null) {
            return;
        }
        int permits = Math.max(1, nextFetchRatePermits);
        fetchRateLimiter.acquire(permits);
    }

    private void unassignPendingFinishedPartitions() throws ConsumerException, ConfigurationException {
        if (pendingUnassignPartitions.isEmpty()) {
            return;
        }
        pendingUnassignPartitions.forEach(pscSourceReaderMetrics::removeRecordsLagMetric);
        unassignPartitions(pendingUnassignPartitions);
        pendingUnassignPartitions.clear();
    }

    @Override
    public RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> fetch() throws IOException {
        try {
            unassignPendingFinishedPartitions();
        } catch (ConsumerException | ConfigurationException e) {
            throw new RuntimeException("Failed to unassign finished partitions", e);
        }

        // Pace MemQ/Kafka downloads: acquire before poll so fetchObjectToInputStream cannot run
        // ahead of the configured record budget.
        acquireFetchRatePermitsBeforePoll();

        PscConsumerPollMessageIterator<byte[], byte[]> pollIterator;
        try {
            pollIterator = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
        } catch (ConsumerException e) {
            // IllegalStateException will be thrown if the consumer is not assigned any partitions.
            // This happens if all assigned partitions are invalid or empty (starting offset >=
            // stopping offset). We just mark empty partitions as finished and return an empty
            // record container, and this consumer will be closed by SplitFetcherManager.
            if (e.getCause() != null
                    && (e.getCause().getClass().equals(IllegalStateException.class)
                            || e.getCause().getClass().equals(WakeupException.class))) {
                LOG.warn(
                        "Caught IllegalStateException or WakeupException in poll(), marking partitions as finished",
                        e);
                nextFetchRatePermits = 1;
                PscPartitionSplitRecords recordsBySplits =
                        PscPartitionSplitRecords.empty(pscSourceReaderMetrics);
                markEmptySplitsAsFinished(recordsBySplits);
                return recordsBySplits;
            } else {
                LOG.error("Unrecoverable ConsumerException caught in poll()", e);
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            LOG.error("Unrecoverable Exception caught in poll()", e);
            throw new RuntimeException(e);
        }

        // Stream records from the poll iterator — do not call asList() / PscConsumerMessagesIterable,
        // which materializes every raw payload onto the heap before Flink can emit or backpressure.
        PscPartitionSplitRecords recordsBySplits =
                new PscPartitionSplitRecords(
                        pollIterator,
                        stoppingOffsets,
                        pscSourceReaderMetrics,
                        pendingUnassignPartitions,
                        emittedCount -> nextFetchRatePermits = Math.max(1, emittedCount));

        markEmptySplitsAsFinished(recordsBySplits);

        // Update numBytesIn (best-effort; streaming path updates as records are read)
        pscSourceReaderMetrics.updateNumBytesInCounter();

        return recordsBySplits;
    }

    private void markEmptySplitsAsFinished(PscPartitionSplitRecords recordsBySplits) {
        // Some splits are discovered as empty when handling split additions. These splits should be
        // added to finished splits to clean up states in split fetcher and source reader.
        if (!emptySplits.isEmpty()) {
            recordsBySplits.finishedSplits.addAll(emptySplits);
            emptySplits.clear();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PscTopicUriPartitionSplit> splitsChange) {
        // Get all the partition assignments and stopping offsets.
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        // Assignment.
        List<TopicUriPartition> newPartitionAssignments = new ArrayList<>();
        // Starting offsets.
        List<TopicUriPartition> partitionsStartingFromEarliest = new ArrayList<>();
        List<TopicUriPartition> partitionsStartingFromLatest = new ArrayList<>();
        Map<TopicUriPartition, Long> partitionsStartingFromSpecifiedOffsets = new HashMap<>();
        // Stopping offsets.
        List<TopicUriPartition> partitionsStoppingAtLatest = new ArrayList<>();
        Set<TopicUriPartition> partitionsStoppingAtCommitted = new HashSet<>();

        // Parse the starting and stopping offsets.
        splitsChange
                .splits()
                .forEach(
                        s -> {
                            newPartitionAssignments.add(s.getTopicUriPartition());
                            parseStartingOffsets(
                                    s,
                                    partitionsStartingFromEarliest,
                                    partitionsStartingFromLatest,
                                    partitionsStartingFromSpecifiedOffsets);
                            parseStoppingOffsets(
                                    s, partitionsStoppingAtLatest, partitionsStoppingAtCommitted);
                            // Track the new topic partition in metrics
                            pscSourceReaderMetrics.registerTopicUriPartition(s.getTopicUriPartition());
                        });

        // Assign new partitions.
        try {
            newPartitionAssignments.addAll(consumer.assignment());
            consumer.assign(newPartitionAssignments);
        } catch (ConsumerException | ConfigurationException e) {
            throw new RuntimeException("Failed to assign PscConsumer", e);
        }

        // Metric registration
        try {
            maybeRegisterPscConsumerMetrics(props, pscSourceReaderMetrics, consumer);
            this.pscSourceReaderMetrics.registerNumBytesIn(consumer);
        } catch (ClientException e) {
            throw new RuntimeException("Failed to register metrics for PscConsumer", e);
        }

        try {
            // Seek on the newly assigned partitions to their stating offsets.
            seekToStartingOffsets(
                    partitionsStartingFromEarliest,
                    partitionsStartingFromLatest,
                    partitionsStartingFromSpecifiedOffsets);
            // Setup the stopping offsets.
            acquireAndSetStoppingOffsets(partitionsStoppingAtLatest, partitionsStoppingAtCommitted);

            // After acquiring the starting and stopping offsets, remove the empty splits if necessary.
            removeEmptySplits();
        } catch (ConfigurationException | ConsumerException e) {
            throw new RuntimeException("Failed to handle split changes", e);
        }

        try {
            maybeLogSplitChangesHandlingResult(splitsChange);
        } catch (ConsumerException e) {
            throw new RuntimeException("Failed to log split changes handling result", e);
        }
    }

    @Override
    public void wakeUp() {
        consumer.wakeup();
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<PscTopicUriPartitionSplit> splitsToPause,
            Collection<PscTopicUriPartitionSplit> splitsToResume) {
        try {
            consumer.resume(
                    splitsToResume.stream()
                            .map(PscTopicUriPartitionSplit::getTopicUriPartition)
                            .collect(Collectors.toList()));
            consumer.pause(
                    splitsToPause.stream()
                            .map(PscTopicUriPartitionSplit::getTopicUriPartition)
                            .collect(Collectors.toList()));
        } catch (ConsumerException e) {
            throw new RuntimeException("Failed to pause/resume", e);
        }
    }

    // ---------------

    public void notifyCheckpointComplete(
            Collection<MessageId> offsetsToCommit,
            OffsetCommitCallback offsetCommitCallback) throws ConfigurationException, ConsumerException {
        consumer.commitAsync(offsetsToCommit, offsetCommitCallback);
    }

    @VisibleForTesting
    PscConsumer<byte[], byte[]> consumer() {
        return consumer;
    }

    @VisibleForTesting
    @Nullable
    RateLimiter fetchRateLimiter() {
        return fetchRateLimiter;
    }

    @VisibleForTesting
    int nextFetchRatePermits() {
        return nextFetchRatePermits;
    }

    // --------------- private helper method ----------------------

    /**
     * This Method performs Null and empty Rack Id validation and sets the rack id to the
     * client.rack Consumer Config.
     *
     * @param consumerProps Consumer Property.
     * @param rackId Rack Id's.
     */
    @VisibleForTesting
    void setConsumerClientRack(Properties consumerProps, String rackId) {
        if (rackId != null && !rackId.isEmpty()) {
            // this is going to pass through to backend consumer config
            consumerProps.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_RACK, rackId);
        }
    }

    private void parseStartingOffsets(
            PscTopicUriPartitionSplit split,
            List<TopicUriPartition> partitionsStartingFromEarliest,
            List<TopicUriPartition> partitionsStartingFromLatest,
            Map<TopicUriPartition, Long> partitionsStartingFromSpecifiedOffsets) {
        TopicUriPartition tp = split.getTopicUriPartition();
        // Parse starting offsets.
        if (split.getStartingOffset() == PscTopicUriPartitionSplit.EARLIEST_OFFSET) {
            partitionsStartingFromEarliest.add(tp);
        } else if (split.getStartingOffset() == PscTopicUriPartitionSplit.LATEST_OFFSET) {
            partitionsStartingFromLatest.add(tp);
        } else if (split.getStartingOffset() == PscTopicUriPartitionSplit.COMMITTED_OFFSET) {
            // Do nothing here, the consumer will first try to get the committed offsets of
            // these partitions by default.
        } else {
            partitionsStartingFromSpecifiedOffsets.put(tp, split.getStartingOffset());
        }
    }

    private void parseStoppingOffsets(
            PscTopicUriPartitionSplit split,
            List<TopicUriPartition> partitionsStoppingAtLatest,
            Set<TopicUriPartition> partitionsStoppingAtCommitted) {
        TopicUriPartition tp = split.getTopicUriPartition();
        split.getStoppingOffset()
                .ifPresent(
                        stoppingOffset -> {
                            if (stoppingOffset >= 0) {
                                stoppingOffsets.put(tp, stoppingOffset);
                            } else if (stoppingOffset == PscTopicUriPartitionSplit.LATEST_OFFSET) {
                                partitionsStoppingAtLatest.add(tp);
                            } else if (stoppingOffset == PscTopicUriPartitionSplit.COMMITTED_OFFSET) {
                                partitionsStoppingAtCommitted.add(tp);
                            } else {
                                // This should not happen.
                                throw new FlinkRuntimeException(
                                        String.format(
                                                "Invalid stopping offset %d for partition %s",
                                                stoppingOffset, tp));
                            }
                        });
    }

    private void seekToStartingOffsets(
            List<TopicUriPartition> partitionsStartingFromEarliest,
            List<TopicUriPartition> partitionsStartingFromLatest,
            Map<TopicUriPartition, Long> partitionsStartingFromSpecifiedOffsets) throws ConsumerException {

        if (!partitionsStartingFromEarliest.isEmpty()) {
            LOG.info("Seeking starting offsets to beginning: {}", partitionsStartingFromEarliest);
            consumer.seekToBeginning(partitionsStartingFromEarliest);
        }

        if (!partitionsStartingFromLatest.isEmpty()) {
            LOG.info("Seeking starting offsets to end: {}", partitionsStartingFromLatest);
            consumer.seekToEnd(partitionsStartingFromLatest);
        }

        if (!partitionsStartingFromSpecifiedOffsets.isEmpty()) {
            LOG.info(
                    "Seeking starting offsets to specified offsets: {}",
                    partitionsStartingFromSpecifiedOffsets);
            partitionsStartingFromSpecifiedOffsets.forEach((tup, offset) -> {
                try {
                    consumer.seekToOffset(tup, offset);
                } catch (ConsumerException e) {
                    throw new RuntimeException(String.format("Failed to seek to offset for TopicUriPartition=%s, offset=%s", tup, offset), e);
                }
            });
        }
    }

    private void acquireAndSetStoppingOffsets(
            List<TopicUriPartition> partitionsStoppingAtLatest,
            Set<TopicUriPartition> partitionsStoppingAtCommitted) throws ConfigurationException, ConsumerException {
        Map<TopicUriPartition, Long> endOffset = consumer.endOffsets(partitionsStoppingAtLatest);
        stoppingOffsets.putAll(endOffset);
        if (!partitionsStoppingAtCommitted.isEmpty()) {
            retryOnWakeup(
                            () -> {
                                try {
                                    return consumer.committed(partitionsStoppingAtCommitted);
                                } catch (ConsumerException | ConfigurationException e) {
                                    throw new RuntimeException("Failed to get committed offsets for " + partitionsStoppingAtCommitted, e);
                                }
                            },
                            "getting committed offset as stopping offsets")
                    .forEach(
                            (messageId) -> {
                                Preconditions.checkState(
                                        messageId.getOffset() >= 0,
                                        String.format(
                                                "Partition %s should stop at committed offset. "
                                                        + "But there is no committed offset of this partition for group %s",
                                                messageId.getTopicUriPartition().getPartition(), groupId)
                                );
                                stoppingOffsets.put(messageId.getTopicUriPartition(), messageId.getOffset());
                            });
        }
    }

    private void removeEmptySplits() throws ConsumerException, ConfigurationException {
        List<TopicUriPartition> emptyPartitions = new ArrayList<>();
        // If none of the partitions have any records,
        for (TopicUriPartition tp : consumer.assignment()) {
            if (retryOnWakeup(
                            () -> {
                                try {
                                    return consumer.position(tp);
                                } catch (ConsumerException e) {
                                    throw new RuntimeException("Failed to get position", e);
                                }
                            },
                            "getting starting offset to check if split is empty")
                    >= getStoppingOffset(tp)) {
                emptyPartitions.add(tp);
            }
        }
        if (!emptyPartitions.isEmpty()) {
            LOG.debug(
                    "These assigning splits are empty and will be marked as finished in later fetch: {}",
                    emptyPartitions);
            // Add empty partitions to empty split set for later cleanup in fetch()
            emptySplits.addAll(
                    emptyPartitions.stream()
                            .map(PscTopicUriPartitionSplit::toSplitId)
                            .collect(Collectors.toSet()));
            // Un-assign partitions from PSC consumer
            unassignPartitions(emptyPartitions);
        }
    }

    private void maybeLogSplitChangesHandlingResult(
            SplitsChange<PscTopicUriPartitionSplit> splitsChange) throws ConsumerException {
        if (LOG.isDebugEnabled()) {
            StringJoiner splitsInfo = new StringJoiner(",");
            Set<TopicUriPartition> assignment = consumer.assignment();
            for (PscTopicUriPartitionSplit split : splitsChange.splits()) {
                if (!assignment.contains(split.getTopicUriPartition())) {
                    continue;
                }

                long startingOffset =
                        retryOnWakeup(
                                () -> {
                                    try {
                                        return consumer.position(split.getTopicUriPartition());
                                    } catch (ConsumerException e) {
                                        throw new RuntimeException("Failed to get position for " + split.getTopicUriPartition(), e);
                                    }
                                },
                                "logging starting position");
                long stoppingOffset = getStoppingOffset(split.getTopicUriPartition());
                splitsInfo.add(
                        String.format(
                                "[%s, start:%d, stop: %d]",
                                split.getTopicUriPartition(), startingOffset, stoppingOffset));
            }
            LOG.debug("SplitsChange handling result: {}", splitsInfo);
        }
    }

    private void unassignPartitions(Collection<TopicUriPartition> partitionsToUnassign) throws ConsumerException, ConfigurationException {
        Collection<TopicUriPartition> newAssignment = new HashSet<>(consumer.assignment());
        newAssignment.removeAll(partitionsToUnassign);
        consumer.assign(newAssignment);
    }

    private String createConsumerClientId(Properties props) {
        String prefix = props.getProperty(PscSourceOptions.CLIENT_ID_PREFIX.key());
        return prefix + "-" + subtaskId;
    }

    private long getStoppingOffset(TopicUriPartition tp) {
        return stoppingOffsets.getOrDefault(tp, Long.MAX_VALUE);
    }

    private void maybeRegisterPscConsumerMetrics(
            Properties props,
            PscSourceReaderMetrics pscSourceReaderMetrics,
            PscConsumer<?, ?> consumer) throws ClientException {
        final Boolean needToRegister =
                PscSourceOptions.getOption(
                        props,
                        PscSourceOptions.REGISTER_PSC_CONSUMER_METRICS,
                        Boolean::parseBoolean);
        if (needToRegister) {
            pscSourceReaderMetrics.registerPscConsumerMetrics(consumer);
        }
    }

    /**
     * Catch {@link WakeupException} in PSC consumer call and retry the invocation on exception.
     *
     * <p>This helper function handles a race condition as below:
     *
     * <ol>
     *   <li>Fetcher thread finishes a {@link PscConsumer#poll(Duration)} call
     *   <li>Task thread assigns new splits so invokes {@link #wakeUp()}, then the wakeup is
     *       recorded and held by the consumer
     *   <li>Later fetcher thread invokes {@link #handleSplitsChanges(SplitsChange)}, and
     *       interactions with consumer will throw {@link com.pinterest.psc.exception.consumer.WakeupException} because of the previously
     *       held wakeup in the consumer
     * </ol>
     *
     * <p>Under this case we need to catch the {@link } and retry the operation.
     */
    private <V> V retryOnWakeup(Supplier<V> consumerCall, String description) {
        try {
            return consumerCall.get();
        } catch (RuntimeException we) {
            if (!(we.getCause() instanceof WakeupException)) {
                throw we;
            }
            LOG.info(
                    "Caught WakeupException while executing PSC consumer call for {}. Will retry the consumer call.",
                    description);
            return consumerCall.get();
        }
    }

    // ---------------- private helper class ------------------------

    /**
     * Streams {@link PscConsumerPollMessageIterator} records into Flink without materializing the
     * full poll into a {@code List} via {@code asList()}.
     *
     * <p>Records are grouped into splits on the fly: {@link #nextSplit()} starts a partition from
     * the peeked message; {@link #nextRecordFromSplit()} emits consecutive messages for that
     * partition until the partition changes or the iterator is exhausted.
     */
    private static class PscPartitionSplitRecords
            implements RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> {

        private final Set<String> finishedSplits = new HashSet<>();
        private final Map<TopicUriPartition, Long> stoppingOffsets;
        private final PscSourceReaderMetrics metrics;
        private final List<TopicUriPartition> finishedPartitionsForUnassign;
        private final IntConsumer onFinishedEmitting;

        @Nullable private final PscConsumerPollMessageIterator<byte[], byte[]> pollIterator;
        @Nullable private PscConsumerMessage<byte[], byte[]> peekedMessage;
        private boolean iteratorExhausted;

        private TopicUriPartition currentTopicPartition;
        private Long currentSplitStoppingOffset;
        private PscSourceReaderMetrics.Offset currentOffsetTracker;
        private int emittedCount;

        private boolean reportedEmitCount;

        private PscPartitionSplitRecords(
                @Nullable PscConsumerPollMessageIterator<byte[], byte[]> pollIterator,
                Map<TopicUriPartition, Long> stoppingOffsets,
                PscSourceReaderMetrics metrics,
                List<TopicUriPartition> finishedPartitionsForUnassign,
                IntConsumer onFinishedEmitting) {
            this.pollIterator = pollIterator;
            this.stoppingOffsets = stoppingOffsets;
            this.metrics = metrics;
            this.finishedPartitionsForUnassign = finishedPartitionsForUnassign;
            this.onFinishedEmitting = onFinishedEmitting;
            this.iteratorExhausted = pollIterator == null;
        }

        private static PscPartitionSplitRecords empty(PscSourceReaderMetrics metrics) {
            return new PscPartitionSplitRecords(
                    null, new HashMap<>(), metrics, new ArrayList<>(), count -> {});
        }

        private void reportEmittedCountOnce() {
            if (!reportedEmitCount) {
                reportedEmitCount = true;
                onFinishedEmitting.accept(emittedCount);
            }
        }

        private void ensurePeek() {
            if (peekedMessage != null || iteratorExhausted) {
                return;
            }
            while (pollIterator != null && pollIterator.hasNext()) {
                PscConsumerMessage<byte[], byte[]> next = pollIterator.next();
                TopicUriPartition tp = next.getMessageId().getTopicUriPartition();
                // Drop messages for splits already finished in this poll (stopping offset reached).
                if (finishedSplits.contains(PscTopicUriPartitionSplit.toSplitId(tp))) {
                    continue;
                }
                peekedMessage = next;
                return;
            }
            iteratorExhausted = true;
            closeIteratorQuietly();
            reportEmittedCountOnce();
        }

        private void closeIteratorQuietly() {
            if (pollIterator == null) {
                return;
            }
            try {
                pollIterator.close();
            } catch (IOException e) {
                LOG.warn("Failed to close poll message iterator", e);
            }
        }

        private void maybeFinishSplitAtOffset(long offset) {
            if (offset < currentSplitStoppingOffset - 1) {
                return;
            }
            String splitId = PscTopicUriPartitionSplit.toSplitId(currentTopicPartition);
            if (finishedSplits.add(splitId)) {
                finishedPartitionsForUnassign.add(currentTopicPartition);
                LOG.debug(
                        "{} has reached stopping offset {}, current offset is {}",
                        currentTopicPartition,
                        currentSplitStoppingOffset,
                        offset);
            }
        }

        @Nullable
        @Override
        public String nextSplit() {
            ensurePeek();
            if (peekedMessage == null) {
                currentTopicPartition = null;
                currentSplitStoppingOffset = null;
                currentOffsetTracker = null;
                return null;
            }
            currentTopicPartition = peekedMessage.getMessageId().getTopicUriPartition();
            currentSplitStoppingOffset =
                    stoppingOffsets.getOrDefault(currentTopicPartition, Long.MAX_VALUE);
            currentOffsetTracker = metrics.getOffsetTracker(currentTopicPartition);
            return currentTopicPartition.toString();
        }

        @Nullable
        @Override
        public PscConsumerMessage<byte[], byte[]> nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentTopicPartition,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            ensurePeek();
            if (peekedMessage == null) {
                return null;
            }
            TopicUriPartition messageTp = peekedMessage.getMessageId().getTopicUriPartition();
            if (!messageTp.equals(currentTopicPartition)) {
                return null;
            }

            final PscConsumerMessage<byte[], byte[]> message = peekedMessage;
            peekedMessage = null;
            final long offset = message.getMessageId().getOffset();

            // Only emit records before the stopping offset (same contract as before).
            if (offset >= currentSplitStoppingOffset) {
                maybeFinishSplitAtOffset(offset);
                return null;
            }

            currentOffsetTracker.currentOffset = offset;
            emittedCount++;
            maybeFinishSplitAtOffset(offset);
            return message;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }

        @Override
        public void recycle() {
            // Prefer closing any remaining iterator state once Flink is done with this batch.
            if (!iteratorExhausted) {
                iteratorExhausted = true;
                peekedMessage = null;
                closeIteratorQuietly();
            }
            reportEmittedCountOnce();
        }
    }
}
