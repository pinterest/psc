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
import com.pinterest.psc.consumer.PscConsumerMessagesIterable;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** A {@link SplitReader} implementation that reads records from PSC TopicUriPartitions. */
@Internal
public class PscTopicUriPartitionSplitReader
        implements SplitReader<PscConsumerMessage<byte[], byte[]>, PscTopicUriPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PscTopicUriPartitionSplitReader.class);
    private static final long POLL_TIMEOUT = 10000L;

    private final PscConsumer<byte[], byte[]> consumer;
    private final Map<TopicUriPartition, Long> stoppingOffsets;
    private final String groupId;
    private final int subtaskId;

    private final PscSourceReaderMetrics pscSourceReaderMetrics;

    // Tracking empty splits that has not been added to finished splits in fetch()
    private final Set<String> emptySplits = new HashSet<>();
    private final Properties props;

    public PscTopicUriPartitionSplitReader(
            Properties props,
            SourceReaderContext context,
            PscSourceReaderMetrics pscSourceReaderMetrics) throws ConfigurationException, ClientException {
        this.props = props;
        this.subtaskId = context.getIndexOfSubtask();
        this.pscSourceReaderMetrics = pscSourceReaderMetrics;
        Properties consumerProps = new Properties();
        consumerProps.putAll(props);
        consumerProps.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, createConsumerClientId(props));
        this.consumer = new PscConsumer<>(PscConfigurationUtils.propertiesToPscConfiguration(consumerProps));
        this.stoppingOffsets = new HashMap<>();
        this.groupId = consumerProps.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
    }

    @Override
    public RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> fetch() throws IOException {
        PscConsumerMessagesIterable<byte[], byte[]> consumerMessagesIterable;
        try {
            consumerMessagesIterable = new PscConsumerMessagesIterable<>(consumer.poll(Duration.ofMillis(POLL_TIMEOUT)));
        } catch (ConsumerException e) {
            // IllegalStateException will be thrown if the consumer is not assigned any partitions.
            // This happens if all assigned partitions are invalid or empty (starting offset >=
            // stopping offset). We just mark empty partitions as finished and return an empty
            // record container, and this consumer will be closed by SplitFetcherManager.
            PscPartitionSplitRecords recordsBySplits =
                    new PscPartitionSplitRecords(
                            PscConsumerMessagesIterable.emptyIterable(), pscSourceReaderMetrics);
            markEmptySplitsAsFinished(recordsBySplits);
            return recordsBySplits;
        }
        PscPartitionSplitRecords recordsBySplits =
                new PscPartitionSplitRecords(consumerMessagesIterable, pscSourceReaderMetrics);
        List<TopicUriPartition> finishedPartitions = new ArrayList<>();
        for (TopicUriPartition tp : consumerMessagesIterable.getTopicUriPartitions()) {
            long stoppingOffset = getStoppingOffset(tp);
            final List<PscConsumerMessage<byte[], byte[]>> recordsFromPartition =
                    consumerMessagesIterable.getMessagesForTopicUriPartition(tp);

            if (recordsFromPartition.size() > 0) {
                final PscConsumerMessage<byte[], byte[]> lastRecord =
                        recordsFromPartition.get(recordsFromPartition.size() - 1);

                // After processing a record with offset of "stoppingOffset - 1", the split reader
                // should not continue fetching because the record with stoppingOffset may not
                // exist. Keep polling will just block forever.
                if (lastRecord.getMessageId().getOffset() >= stoppingOffset - 1) {
                    recordsBySplits.setPartitionStoppingOffset(tp, stoppingOffset);
                    finishSplitAtRecord(
                            tp,
                            stoppingOffset,
                            lastRecord.getMessageId().getOffset(),
                            finishedPartitions,
                            recordsBySplits);
                }
            }
            // Track this partition's record lag if it never appears before
            pscSourceReaderMetrics.maybeAddRecordsLagMetric(consumer, tp);
        }

        markEmptySplitsAsFinished(recordsBySplits);

        // Unassign the partitions that has finished.
        if (!finishedPartitions.isEmpty()) {
            finishedPartitions.forEach(pscSourceReaderMetrics::removeRecordsLagMetric);
            try {
                unassignPartitions(finishedPartitions);
            } catch (ConsumerException | ConfigurationException e) {
                throw new RuntimeException("Failed to unassign partitions", e);
            }
        }

        // Update numBytesIn
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
        Map<TopicUriPartition, Long> partitionsStartingFromSpecifiedOffsets = new HashMap<>();
        List<TopicUriPartition> partitionsStartingFromEarliest = new ArrayList<>();
        List<TopicUriPartition> partitionsStartingFromLatest = new ArrayList<>();
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

        maybeLogSplitChangesHandlingResult(splitsChange);
    }

    @Override
    public void wakeUp() {
        consumer.wakeup();
    }

    @Override
    public void close() throws Exception {
        consumer.close();
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

    // --------------- private helper method ----------------------

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
            SplitsChange<PscTopicUriPartitionSplit> splitsChange) {
        if (LOG.isDebugEnabled()) {
            StringJoiner splitsInfo = new StringJoiner(",");
            for (PscTopicUriPartitionSplit split : splitsChange.splits()) {
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

    private void finishSplitAtRecord(
            TopicUriPartition tp,
            long stoppingOffset,
            long currentOffset,
            List<TopicUriPartition> finishedPartitions,
            PscPartitionSplitRecords recordsBySplits) {
        LOG.debug(
                "{} has reached stopping offset {}, current offset is {}",
                tp,
                stoppingOffset,
                currentOffset);
        finishedPartitions.add(tp);
        recordsBySplits.addFinishedSplit(PscTopicUriPartitionSplit.toSplitId(tp));
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

    private static class PscPartitionSplitRecords
            implements RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> {

        private final Set<String> finishedSplits = new HashSet<>();
        private final Map<TopicUriPartition, Long> stoppingOffsets = new HashMap<>();
        private final PscConsumerMessagesIterable<byte[], byte[]> consumerMessagesIterable;
        private final PscSourceReaderMetrics metrics;
        private final Iterator<TopicUriPartition> splitIterator;
        private Iterator<PscConsumerMessage<byte[], byte[]>> recordIterator;
        private TopicUriPartition currentTopicPartition;
        private Long currentSplitStoppingOffset;

        private PscPartitionSplitRecords(
                PscConsumerMessagesIterable<byte[], byte[]> consumerMessagesIterable, PscSourceReaderMetrics metrics) {
            this.consumerMessagesIterable = consumerMessagesIterable;
            this.splitIterator = consumerMessagesIterable.getTopicUriPartitions().iterator();
            this.metrics = metrics;
        }

        private void setPartitionStoppingOffset(
                TopicUriPartition topicUriPartition, long stoppingOffset) {
            stoppingOffsets.put(topicUriPartition, stoppingOffset);
        }

        private void addFinishedSplit(String splitId) {
            finishedSplits.add(splitId);
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                currentTopicPartition = splitIterator.next();
                recordIterator = consumerMessagesIterable.getMessagesForTopicUriPartition(currentTopicPartition).iterator();
                currentSplitStoppingOffset =
                        stoppingOffsets.getOrDefault(currentTopicPartition, Long.MAX_VALUE);
                return currentTopicPartition.toString();
            } else {
                currentTopicPartition = null;
                recordIterator = null;
                currentSplitStoppingOffset = null;
                return null;
            }
        }

        @Nullable
        @Override
        public PscConsumerMessage<byte[], byte[]> nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentTopicPartition,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (recordIterator.hasNext()) {
                final PscConsumerMessage<byte[], byte[]> message = recordIterator.next();
                // Only emit records before stopping offset
                if (message.getMessageId().getOffset() < currentSplitStoppingOffset) {
                    metrics.recordCurrentOffset(currentTopicPartition, message.getMessageId().getOffset());
                    return message;
                }
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
