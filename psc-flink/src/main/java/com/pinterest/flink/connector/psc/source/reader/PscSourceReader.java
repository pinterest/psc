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
import com.pinterest.flink.connector.psc.source.reader.fetcher.PscSourceFetcherManager;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplitState;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumerMessage;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** The source reader for PSC TopicUriPartitions. */
@Internal
public class PscSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
        PscConsumerMessage<byte[], byte[]>, T, PscTopicUriPartitionSplit, PscTopicUriPartitionSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(PscSourceReader.class);
    // These maps need to be concurrent because it will be accessed by both the main thread
    // and the split fetcher thread in the callback.
    private final SortedMap<Long, Map<TopicUriPartition, MessageId>> offsetsToCommit;
    private final ConcurrentMap<TopicUriPartition, MessageId> offsetsOfFinishedSplits;
    private final PscSourceReaderMetrics pscSourceReaderMetrics;
    private final boolean commitOffsetsOnCheckpoint;

    public PscSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>>>
                    elementsQueue,
            PscSourceFetcherManager pscSourceFetcherManager,
            RecordEmitter<PscConsumerMessage<byte[], byte[]>, T, PscTopicUriPartitionSplitState>
                    recordEmitter,
            Configuration config,
            SourceReaderContext context,
            PscSourceReaderMetrics pscSourceReaderMetrics) {
        super(elementsQueue, pscSourceFetcherManager, recordEmitter, config, context);
        this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.offsetsOfFinishedSplits = new ConcurrentHashMap<>();
        this.pscSourceReaderMetrics = pscSourceReaderMetrics;
        this.commitOffsetsOnCheckpoint =
                config.get(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT);
        if (!commitOffsetsOnCheckpoint) {
            LOG.warn(
                    "Offset commit on checkpoint is disabled. "
                            + "Consuming offset will not be reported back to PubSub cluster.");
        }
    }

    @Override
    protected void onSplitFinished(Map<String, PscTopicUriPartitionSplitState> finishedSplitIds) {
        finishedSplitIds.forEach(
                (ignored, splitState) -> {
                    if (splitState.getCurrentOffset() >= 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.getTopicUriPartition(),
                                // last processed offset. split.getCurrentOffset() is the next offset to be processed
                                new MessageId(splitState.getTopicUriPartition(), splitState.getCurrentOffset() - 1));
                    }
                });
    }

    @Override
    public List<PscTopicUriPartitionSplit> snapshotState(long checkpointId) {
        List<PscTopicUriPartitionSplit> splits = super.snapshotState(checkpointId);
        if (!commitOffsetsOnCheckpoint) {
            return splits;
        }

        if (splits.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
            offsetsToCommit.put(checkpointId, Collections.emptyMap());
        } else {
            Map<TopicUriPartition, MessageId> offsetsMap =
                    offsetsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());
            // Put the offsets of the active splits.
            for (PscTopicUriPartitionSplit split : splits) {
                // If the checkpoint is triggered before the partition starting offsets
                // is retrieved, do not commit the offsets for those partitions.
                if (split.getStartingOffset() >= 0) {
                    offsetsMap.put(
                            split.getTopicUriPartition(),
                            // last processed offset. split.getStartingOffset() is the next offset to be processed
                            new MessageId(split.getTopicUriPartition(), split.getStartingOffset() - 1));
                }
            }
            // Put offsets of all the finished splits.
            offsetsMap.putAll(offsetsOfFinishedSplits);
        }
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Committing offsets for checkpoint {}", checkpointId);
        if (!commitOffsetsOnCheckpoint) {
            return;
        }

        Map<TopicUriPartition, MessageId> committedPartitions =
                offsetsToCommit.get(checkpointId);
        if (committedPartitions == null) {
            LOG.debug(
                    "Offsets for checkpoint {} either do not exist or have already been committed.",
                    checkpointId);
            return;
        }

        ((PscSourceFetcherManager) splitFetcherManager)
                .commitOffsets(
                        committedPartitions.values(),
                        (ignored, e) -> {
                            // The offset commit here is needed by the external monitoring. It won't
                            // break Flink job's correctness if we fail to commit the offset here.
                            if (e != null) {
                                pscSourceReaderMetrics.recordFailedCommit();
                                LOG.warn(
                                        "Failed to commit consumer offsets for checkpoint {}",
                                        checkpointId,
                                        e);
                            } else {
                                LOG.debug(
                                        "Successfully committed offsets for checkpoint {}",
                                        checkpointId);
                                pscSourceReaderMetrics.recordSucceededCommit();
                                // If the finished topic partition has been committed, we remove it
                                // from the offsets of the finished splits map.
                                committedPartitions.forEach(
                                        (tp, offset) ->
                                                pscSourceReaderMetrics.recordCommittedOffset(
                                                        tp, offset.getOffset() + 1));   // offset.getOffset() is the last processed offset
                                offsetsOfFinishedSplits
                                        .entrySet()
                                        .removeIf(
                                                entry ->
                                                        committedPartitions.containsKey(
                                                                entry.getKey()));
                                while (!offsetsToCommit.isEmpty()
                                        && offsetsToCommit.firstKey() <= checkpointId) {
                                    offsetsToCommit.remove(offsetsToCommit.firstKey());
                                }
                            }
                        });
    }

    @Override
    protected PscTopicUriPartitionSplitState initializedState(PscTopicUriPartitionSplit split) {
        return new PscTopicUriPartitionSplitState(split);
    }

    @Override
    protected PscTopicUriPartitionSplit toSplitType(String splitId, PscTopicUriPartitionSplitState splitState) {
        return splitState.toPscTopicUriPartitionSplit();
    }

    // ------------------------

    @VisibleForTesting
    SortedMap<Long, Map<TopicUriPartition, MessageId>> getOffsetsToCommit() {
        return offsetsToCommit;
    }

    @VisibleForTesting
    int getNumAliveFetchers() {
        return splitFetcherManager.getNumAliveFetchers();
    }
}
