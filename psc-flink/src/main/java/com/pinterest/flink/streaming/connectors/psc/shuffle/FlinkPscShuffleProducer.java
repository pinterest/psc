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

package com.pinterest.flink.streaming.connectors.psc.shuffle;

import com.pinterest.flink.streaming.connectors.psc.FlinkPscErrorCode;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartitionAssigner;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducerMessage;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import com.pinterest.flink.streaming.connectors.psc.FlinkPscException;
import com.pinterest.flink.streaming.connectors.psc.FlinkPscProducer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink PSC Shuffle Producer Function.
 * It is different from {@link FlinkPscProducer} in the way handling elements and watermarks
 */
@Internal
public class FlinkPscShuffleProducer<IN, KEY> extends FlinkPscProducer<IN> {
    private final PscSerializer<IN> pscSerializer;
    private final KeySelector<IN, KEY> keySelector;
    private final int numberOfPartitions;
    private final Map<Integer, Integer> subtaskToPartitionMap;

    FlinkPscShuffleProducer(
            String defaultTopicUri,
            TypeSerializer<IN> typeSerializer,
            Properties configuration,
            KeySelector<IN, KEY> keySelector,
            Semantic semantic,
            int kafkaProducersPoolSize) {
        super(defaultTopicUri, (element, timestamp) -> null, configuration, semantic, kafkaProducersPoolSize);

        this.pscSerializer = new PscSerializer<>(typeSerializer);
        this.keySelector = keySelector;

        Preconditions.checkArgument(
                configuration.getProperty(FlinkPscShuffle.PARTITION_NUMBER) != null,
                "Missing partition number for PSC Shuffle");
        numberOfPartitions = PropertiesUtil.getInt(configuration, FlinkPscShuffle.PARTITION_NUMBER, Integer.MIN_VALUE);
        subtaskToPartitionMap = new HashMap<>();
    }

    /**
     * This is the function invoked to handle each element.
     *
     * @param transaction Transaction state;
     *                    elements are written to pubsub in transactions to guarantee different level of data consistency
     * @param next        Element to handle
     * @param context     Context needed to handle the element
     * @throws FlinkPscException for PSC error
     */
    @Override
    public void invoke(PscTransactionState transaction, IN next, Context context) throws FlinkPscException {
        checkErroneous();

        // write timestamp to pubsub if timestamp is available
        Long timestamp = context.timestamp();

        int[] partitions = getPartitions(transaction);
        int partitionIndex;
        try {
            int subtaskIndex = KeyGroupRangeAssignment
                    .assignKeyToParallelOperator(keySelector.getKey(next), partitions.length, partitions.length);
            partitionIndex = subtaskToPartitionMap.get(subtaskIndex);
        } catch (Exception e) {
            throw new RuntimeException("Fail to assign a partition number to record", e);
        }

        PscProducerMessage<byte[], byte[]> pscProducerMessage = new PscProducerMessage<>(
                defaultTopicUri,
                partitionIndex,
                null,
                pscSerializer.serializeRecord(next, timestamp),
                timestamp);

        pendingRecords.incrementAndGet();
        try {
            transaction.getProducer().send(pscProducerMessage, callback);
        } catch (ProducerException | ConfigurationException e) {
            throw new FlinkPscException(
                    FlinkPscErrorCode.EXTERNAL_ERROR,
                    String.format("Failed to send message %s using PSC Producer.", pscProducerMessage.toString(false)),
                    e
            );
        }
    }

    /**
     * This is the function invoked to handle each watermark.
     *
     * @param watermark Watermark to handle
     * @throws FlinkPscException For PSC error
     */
    public void invoke(Watermark watermark) throws FlinkPscException {
        checkErroneous();
        PscTransactionState transaction = currentTransaction();

        int[] partitions = getPartitions(transaction);
        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        // broadcast watermark
        long timestamp = watermark.getTimestamp();
        for (int partition : partitions) {
            PscProducerMessage<byte[], byte[]> message = new PscProducerMessage<>(
                    defaultTopicUri,
                    partition,
                    null,
                    pscSerializer.serializeWatermark(watermark, subtask),
                    timestamp);

            pendingRecords.incrementAndGet();
            try {
                transaction.getProducer().send(message, callback);
            } catch (ProducerException | ConfigurationException e) {
                throw new FlinkPscException(
                        FlinkPscErrorCode.EXTERNAL_ERROR,
                        String.format("Failed to send message %s using PSC Producer.", message.toString(false)),
                        e
                );
            }
        }
    }

    private int[] getPartitions(PscTransactionState transaction) {
        int[] partitions = topicUriPartitionsMap.get(defaultTopicUri);
        if (partitions == null) {
            partitions = getPartitionsByTopicUri(defaultTopicUri, transaction.getProducer());
            topicUriPartitionsMap.put(defaultTopicUri, partitions);
            for (int i = 0; i < partitions.length; i++) {
                subtaskToPartitionMap.put(
                        PscTopicUriPartitionAssigner.assign(
                                defaultTopicUri, partitions[i], partitions.length),
                        partitions[i]);
            }
        }

        Preconditions.checkArgument(partitions.length == numberOfPartitions);

        return partitions;
    }

    /**
     * Flink PSC Shuffle Serializer.
     */
    public static final class PscSerializer<IN> implements Serializable {
        public static final int TAG_REC_WITH_TIMESTAMP = 0;
        public static final int TAG_REC_WITHOUT_TIMESTAMP = 1;
        public static final int TAG_WATERMARK = 2;

        private static final long serialVersionUID = 2000002L;
        // easy for updating SerDe format later
        private static final int PSC_SHUFFLE_VERSION = 0;

        private final TypeSerializer<IN> serializer;

        private transient DataOutputSerializer dos;

        PscSerializer(TypeSerializer<IN> serializer) {
            this.serializer = serializer;
        }

        /**
         * Format: Version(byte), TAG(byte), [timestamp(long)], record.
         */
        byte[] serializeRecord(IN record, Long timestamp) {
            if (dos == null) {
                dos = new DataOutputSerializer(16);
            }

            try {
                dos.write(PSC_SHUFFLE_VERSION);

                if (timestamp == null) {
                    dos.write(TAG_REC_WITHOUT_TIMESTAMP);
                } else {
                    dos.write(TAG_REC_WITH_TIMESTAMP);
                    dos.writeLong(timestamp);
                }
                serializer.serialize(record, dos);

            } catch (IOException e) {
                throw new RuntimeException("Unable to serialize record", e);
            }

            byte[] ret = dos.getCopyOfBuffer();
            dos.clear();
            return ret;
        }

        /**
         * Format: Version(byte), TAG(byte), subtask(int), timestamp(long).
         */
        byte[] serializeWatermark(Watermark watermark, int subtask) {
            if (dos == null) {
                dos = new DataOutputSerializer(16);
            }

            try {
                dos.write(PSC_SHUFFLE_VERSION);
                dos.write(TAG_WATERMARK);
                dos.writeInt(subtask);
                dos.writeLong(watermark.getTimestamp());
            } catch (IOException e) {
                throw new RuntimeException("Unable to serialize watermark", e);
            }

            byte[] ret = dos.getCopyOfBuffer();
            dos.clear();
            return ret;
        }
    }
}
