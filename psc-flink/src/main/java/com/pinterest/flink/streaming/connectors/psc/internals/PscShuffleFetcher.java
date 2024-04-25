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
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumerMessage;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import com.pinterest.flink.streaming.connectors.psc.PscDeserializationSchema;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.pinterest.flink.streaming.connectors.psc.shuffle.FlinkPscShuffleProducer.PscSerializer.TAG_REC_WITHOUT_TIMESTAMP;
import static com.pinterest.flink.streaming.connectors.psc.shuffle.FlinkPscShuffleProducer.PscSerializer.TAG_REC_WITH_TIMESTAMP;
import static com.pinterest.flink.streaming.connectors.psc.shuffle.FlinkPscShuffleProducer.PscSerializer.TAG_WATERMARK;

/**
 * Fetch data from Psc for Psc Shuffle.
 */
@Internal
public class PscShuffleFetcher<T> extends PscFetcher<T> {
    /**
     * The handler to check and generate watermarks from fetched records.
     **/
    private final WatermarkHandler watermarkHandler;

    /**
     * The schema to convert between PSC's byte messages, and Flink's objects.
     */
    private final PscShuffleElementDeserializer pscShuffleElementDeserializer;

    public PscShuffleFetcher(
            SourceFunction.SourceContext<T> sourceContext,
            Map<PscTopicUriPartition, Long> assignedPscTopicUriPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            String taskNameWithSubtasks,
            PscDeserializationSchema<T> deserializer,
            Properties pscConsumerConfiguration,
            long pollTimeout,
            MetricGroup subtaskMetricGroup,
            MetricGroup consumerMetricGroup,
            boolean useMetrics,
            TypeSerializer<T> typeSerializer,
            int producerParallelism) throws Exception {
        super(
                sourceContext,
                assignedPscTopicUriPartitionsWithInitialOffsets,
                watermarkStrategy,
                processingTimeProvider,
                autoWatermarkInterval,
                userCodeClassLoader,
                taskNameWithSubtasks,
                deserializer,
                pscConsumerConfiguration,
                pollTimeout,
                subtaskMetricGroup,
                consumerMetricGroup,
                useMetrics
        );

        this.pscShuffleElementDeserializer = new PscShuffleElementDeserializer<>(typeSerializer);
        this.watermarkHandler = new WatermarkHandler(producerParallelism);
    }

    @Override
    protected String getFetcherName() {
        return "PSC Shuffle Fetcher";
    }

    @Override
    protected void topicUriPartitionConsumerRecordsHandler(
            PscConsumerMessage<byte[], byte[]> record,
            PscTopicUriPartitionState<T, TopicUriPartition> pscTopicUriPartitionState) throws Exception {

            final PscShuffleElement element = pscShuffleElementDeserializer.deserialize(record);

            // TODO: Do we need to check the end of stream if reaching the end watermark
            // TODO: Currently, if one of the partition sends an end-of-stream signal the fetcher stops running.
            // The current "ending of stream" logic in PscFetcher a bit strange: if any partition has a record
            // signaled as "END_OF_STREAM", the fetcher will stop running. Notice that the signal is coming from
            // the deserializer, which means from PSC data itself. But it is possible that other topics
            // and partitions still have data to read. Finishing reading Partition0 can not guarantee that Partition1
            // also finishes.
            if (element.isRecord()) {
                // timestamp is inherent from upstream
                // If using ProcessTime, timestamp is going to be ignored (upstream does not include timestamp as well)
                // If using IngestionTime, timestamp is going to be overwritten
                // If using EventTime, timestamp is going to be used
                synchronized (checkpointLock) {
                    PscShuffleRecord<T> elementAsRecord = element.asRecord();
                    sourceContext.collectWithTimestamp(
                            elementAsRecord.value,
                            elementAsRecord.timestamp == null ? record.getPublishTimestamp() : elementAsRecord.timestamp);
                    pscTopicUriPartitionState.setOffset(record.getMessageId().getOffset());
                }
            } else if (element.isWatermark()) {
                final PscShuffleWatermark watermark = element.asWatermark();
                Optional<Watermark> newWatermark = watermarkHandler.checkAndGetNewWatermark(watermark);
                newWatermark.ifPresent(sourceContext::emitWatermark);
            }
    }

    /**
     * An element in a PscShuffle. Can be a message or a Watermark.
     */
    @VisibleForTesting
    public abstract static class PscShuffleElement {

        public boolean isRecord() {
            return getClass() == PscShuffleRecord.class;
        }

        public boolean isWatermark() {
            return getClass() == PscShuffleWatermark.class;
        }

        public <T> PscShuffleRecord<T> asRecord() {
            return (PscShuffleRecord<T>) this;
        }

        public PscShuffleWatermark asWatermark() {
            return (PscShuffleWatermark) this;
        }
    }

    /**
     * A watermark element in a PscShuffle. It includes
     * - subtask index where the watermark is coming from
     * - watermark timestamp
     */
    @VisibleForTesting
    public static class PscShuffleWatermark extends PscShuffleElement {
        final int subtask;
        final long watermark;

        PscShuffleWatermark(int subtask, long watermark) {
            this.subtask = subtask;
            this.watermark = watermark;
        }

        public int getSubtask() {
            return subtask;
        }

        public long getWatermark() {
            return watermark;
        }
    }

    /**
     * One value with Type T in a PscShuffle. This stores the value and an optional associated timestamp.
     */
    @VisibleForTesting
    public static class PscShuffleRecord<T> extends PscShuffleElement {
        final T value;
        final Long timestamp;

        PscShuffleRecord(T value) {
            this.value = value;
            this.timestamp = null;
        }

        PscShuffleRecord(long timestamp, T value) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public T getValue() {
            return value;
        }

        public Long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Deserializer for PscShuffleElement.
     */
    @VisibleForTesting
    public static class PscShuffleElementDeserializer<T> implements Serializable {
        private static final long serialVersionUID = 1000001L;

        private final TypeSerializer<T> typeSerializer;

        private transient DataInputDeserializer dis;

        @VisibleForTesting
        public PscShuffleElementDeserializer(TypeSerializer<T> typeSerializer) {
            this.typeSerializer = typeSerializer;
        }

        @VisibleForTesting
        public PscShuffleElement deserialize(PscConsumerMessage<byte[], byte[]> record)
                throws Exception {
            byte[] value = record.getValue();

            if (dis != null) {
                dis.setBuffer(value);
            } else {
                dis = new DataInputDeserializer(value);
            }

            // version byte
            ByteSerializer.INSTANCE.deserialize(dis);
            int tag = ByteSerializer.INSTANCE.deserialize(dis);

            if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
                return new PscShuffleRecord<>(typeSerializer.deserialize(dis));
            } else if (tag == TAG_REC_WITH_TIMESTAMP) {
                return new PscShuffleRecord<>(
                        LongSerializer.INSTANCE.deserialize(dis),
                        typeSerializer.deserialize(dis));
            } else if (tag == TAG_WATERMARK) {
                return new PscShuffleWatermark(
                        IntSerializer.INSTANCE.deserialize(dis), LongSerializer.INSTANCE.deserialize(dis));
            }

            throw new UnsupportedOperationException("Unsupported tag format");
        }
    }

    /**
     * WatermarkHandler to check and generate watermarks from fetched records.
     */
    private static class WatermarkHandler {
        private final int producerParallelism;
        private final Map<Integer, Long> subtaskWatermark;

        private long currentMinWatermark = Long.MIN_VALUE;

        WatermarkHandler(int producerParallelism) {
            this.producerParallelism = producerParallelism;
            this.subtaskWatermark = new HashMap<>(producerParallelism);
        }

        private Optional<Watermark> checkAndGetNewWatermark(PscShuffleWatermark newWatermark) {
            // watermarks is incremental for the same partition and PRODUCER subtask
            Long currentSubTaskWatermark = subtaskWatermark.get(newWatermark.subtask);

            // watermark is strictly increasing
            Preconditions.checkState(
                    (currentSubTaskWatermark == null) || (currentSubTaskWatermark < newWatermark.watermark),
                    "Watermark should always increase: current : new " + currentSubTaskWatermark + ":" + newWatermark.watermark);

            subtaskWatermark.put(newWatermark.subtask, newWatermark.watermark);

            if (subtaskWatermark.values().size() < producerParallelism) {
                return Optional.empty();
            }

            long minWatermark = subtaskWatermark.values().stream().min(Comparator.naturalOrder()).orElse(Long.MIN_VALUE);
            if (currentMinWatermark < minWatermark) {
                currentMinWatermark = minWatermark;
                return Optional.of(new Watermark(minWatermark));
            } else {
                return Optional.empty();
            }
        }
    }
}
