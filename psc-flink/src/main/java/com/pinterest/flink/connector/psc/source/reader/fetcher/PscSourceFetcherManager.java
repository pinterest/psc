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

package com.pinterest.flink.connector.psc.source.reader.fetcher;

import com.pinterest.flink.connector.psc.source.reader.PscTopicUriPartitionSplitReader;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.consumer.OffsetCommitCallback;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The SplitFetcherManager for PSC source. This class is needed to help commit the offsets to
 * PSC using the PscConsumer inside the {@link
 * PscTopicUriPartitionSplitReader}.
 */
@Internal
public class PscSourceFetcherManager
        extends SingleThreadFetcherManager<PscConsumerMessage<byte[], byte[]>, PscTopicUriPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PscSourceFetcherManager.class);

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     * @param splitFinishedHook Hook for handling finished splits in split fetchers.
     */
    public PscSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>>>
                    elementsQueue,
            Supplier<SplitReader<PscConsumerMessage<byte[], byte[]>, PscTopicUriPartitionSplit>>
                    splitReaderSupplier,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, splitFinishedHook);
    }

    public void commitOffsets(
            Collection<MessageId> offsetsToCommit, OffsetCommitCallback callback) {
        LOG.info("Committing offsets {}", offsetsToCommit);
        if (offsetsToCommit.isEmpty()) {
            return;
        }
        SplitFetcher<PscConsumerMessage<byte[], byte[]>, PscTopicUriPartitionSplit> splitFetcher =
                fetchers.get(0);
        if (splitFetcher != null) {
            // The fetcher thread is still running. This should be the majority of the cases.
            enqueueOffsetsCommitTask(splitFetcher, offsetsToCommit, callback);
        } else {
            splitFetcher = createSplitFetcher();
            enqueueOffsetsCommitTask(splitFetcher, offsetsToCommit, callback);
            startFetcher(splitFetcher);
        }
    }

    private void enqueueOffsetsCommitTask(
            SplitFetcher<PscConsumerMessage<byte[], byte[]>, PscTopicUriPartitionSplit> splitFetcher,
            Collection<MessageId> offsetsToCommit,
            OffsetCommitCallback callback) {
        PscTopicUriPartitionSplitReader pscReader =
                (PscTopicUriPartitionSplitReader) splitFetcher.getSplitReader();

        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() throws IOException {
                        try {
                            pscReader.notifyCheckpointComplete(offsetsToCommit, callback);
                        } catch (ConfigurationException | ConsumerException e) {
                            throw new RuntimeException("Notify checkpoint complete failed", e);
                        }
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
