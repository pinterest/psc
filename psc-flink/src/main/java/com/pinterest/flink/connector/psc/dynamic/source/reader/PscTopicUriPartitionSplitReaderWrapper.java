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

package com.pinterest.flink.connector.psc.dynamic.source.reader;

import com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics;
import com.pinterest.flink.connector.psc.source.reader.PscTopicUriPartitionSplitReader;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** This extends to Psc TopicUri Partition Split Reader to wrap split ids with the cluster name. */
@Internal
public class PscTopicUriPartitionSplitReaderWrapper extends PscTopicUriPartitionSplitReader
        implements AutoCloseable {
    private final String kafkaClusterId;

    public PscTopicUriPartitionSplitReaderWrapper(
            Properties props,
            SourceReaderContext context,
            PscSourceReaderMetrics pscSourceReaderMetrics,
            String kafkaClusterId) throws ConfigurationException, ClientException {
        super(props, context, pscSourceReaderMetrics);
        this.kafkaClusterId = kafkaClusterId;
    }

    @Override
    public RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> fetch() throws IOException {
        return new WrappedRecordsWithSplitIds(super.fetch(), kafkaClusterId);
    }

    private static final class WrappedRecordsWithSplitIds
            implements RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> {

        private final RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> delegate;
        private final String kafkaClusterId;

        public WrappedRecordsWithSplitIds(
                RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> delegate,
                String kafkaClusterId) {
            this.delegate = delegate;
            this.kafkaClusterId = kafkaClusterId;
        }

        @Nullable
        @Override
        public String nextSplit() {
            String nextSplit = delegate.nextSplit();
            if (nextSplit == null) {
                return nextSplit;
            } else {
                return kafkaClusterId + "-" + nextSplit;
            }
        }

        @Nullable
        @Override
        public PscConsumerMessage<byte[], byte[]> nextRecordFromSplit() {
            return delegate.nextRecordFromSplit();
        }

        @Override
        public Set<String> finishedSplits() {
            return delegate.finishedSplits().stream()
                    .map(finishedSplit -> kafkaClusterId + "-" + finishedSplit)
                    .collect(Collectors.toSet());
        }

        @Override
        public void recycle() {
            delegate.recycle();
        }
    }
}
