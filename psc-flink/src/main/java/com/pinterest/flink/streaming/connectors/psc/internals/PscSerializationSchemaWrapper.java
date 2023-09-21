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

import com.pinterest.flink.streaming.connectors.psc.PscContextAware;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.psc.producer.PscProducerMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;
import com.pinterest.flink.streaming.connectors.psc.PscSerializationSchema;

import javax.annotation.Nullable;

/**
 * An adapter from old style interfaces such as {@link SerializationSchema},
 * {@link FlinkPscPartitioner} to the
 * {@link PscSerializationSchema}.
 */
public class PscSerializationSchemaWrapper<T> implements PscSerializationSchema<T>, PscContextAware<T> {

    private final FlinkPscPartitioner<T> partitioner;
    private final SerializationSchema<T> serializationSchema;
    private final String topicUri;
    private boolean writeTimestamp;
    private int parallelInstanceId;
    private int numParallelInstances;

    private int[] partitions;

    public PscSerializationSchemaWrapper(
            String topicUri,
            FlinkPscPartitioner<T> partitioner,
            boolean writeTimestamp,
            SerializationSchema<T> serializationSchema) {
        this.partitioner = partitioner;
        this.serializationSchema = serializationSchema;
        this.topicUri = topicUri;
        this.writeTimestamp = writeTimestamp;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        serializationSchema.open(context);
        if (partitioner != null) {
            partitioner.open(parallelInstanceId, numParallelInstances);
        }
    }

    @Override
    public PscProducerMessage<byte[], byte[]> serialize(
            T element,
            @Nullable Long timestamp) {
        byte[] serialized = serializationSchema.serialize(element);
        final Integer partition;
        if (partitioner != null) {
            partition = partitioner.partition(element, null, serialized, topicUri, partitions);
        } else {
            partition = null;
        }

        final Long timestampToWrite;
        if (writeTimestamp) {
            timestampToWrite = timestamp;
        } else {
            timestampToWrite = null;
        }

        return new PscProducerMessage<>(topicUri, partition, null, serialized, timestampToWrite);
    }

    @Override
    public String getTargetTopicUri(T element) {
        return topicUri;
    }

    @Override
    public void setPartitions(int[] partitions) {
        this.partitions = partitions;
    }

    public void setWriteTimestamp(boolean writeTimestamp) {
        this.writeTimestamp = writeTimestamp;
    }

    @Override
    public void setParallelInstanceId(int parallelInstanceId) {
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public void setNumParallelInstances(int numParallelInstances) {
        this.numParallelInstances = numParallelInstances;
    }
}
