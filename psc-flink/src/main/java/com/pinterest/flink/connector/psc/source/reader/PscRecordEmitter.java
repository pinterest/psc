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

import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplitState;
import com.pinterest.psc.consumer.PscConsumerMessage;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import java.io.IOException;

/** The {@link RecordEmitter} implementation for {@link PscSourceReader}. */
@Internal
public class PscRecordEmitter<T>
        implements RecordEmitter<PscConsumerMessage<byte[], byte[]>, T, PscTopicUriPartitionSplitState> {

    private final PscRecordDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    public PscRecordEmitter(PscRecordDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            PscConsumerMessage<byte[], byte[]> consumerMessage,
            SourceOutput<T> output,
            PscTopicUriPartitionSplitState splitState)
            throws Exception {
        try {
            sourceOutputWrapper.setSourceOutput(output);
            sourceOutputWrapper.setTimestamp(consumerMessage.getPublishTimestamp());
            deserializationSchema.deserialize(consumerMessage, sourceOutputWrapper);
            splitState.setCurrentOffset(consumerMessage.getMessageId().getOffset() + 1);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize consumer record due to", e);
        }
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {

        private SourceOutput<T> sourceOutput;
        private long timestamp;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record, timestamp);
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
