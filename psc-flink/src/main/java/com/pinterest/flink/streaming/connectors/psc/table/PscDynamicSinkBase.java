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

package com.pinterest.flink.streaming.connectors.psc.table;

import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * A version-agnostic PSC {@link DynamicTableSink}.
 *
 * <p>The version-specific PSC consumers need to extend this class and
 * override {@link #createPscProducer(String, Properties, SerializationSchema, Optional)}}.
 */

// TODO: migration - remove
@Internal
public abstract class PscDynamicSinkBase implements DynamicTableSink {

    /**
     * Consumed data type of the table.
     */
    protected final DataType consumedDataType;

    /**
     * The PSC topic URI to write to.
     */
    protected final String topicUri;

    /**
     * Properties for the PSC producer.
     */
    protected final Properties properties;

    /**
     * Sink format for encoding records to PSC.
     */
    protected final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    /**
     * Partitioner to select PSC partition for each item.
     */
    protected final Optional<FlinkPscPartitioner<RowData>> partitioner;

    protected PscDynamicSinkBase(
            DataType consumedDataType,
            String topicUri,
            Properties properties,
            Optional<FlinkPscPartitioner<RowData>> partitioner,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        this.consumedDataType = Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null.");
        this.topicUri = Preconditions.checkNotNull(topicUri, "Topic must not be null.");
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.partitioner = Preconditions.checkNotNull(partitioner, "Partitioner must not be null.");
        this.encodingFormat = Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializationSchema =
                this.encodingFormat.createRuntimeEncoder(context, this.consumedDataType);

        final SinkFunction<RowData> pscProducer = createPscProducer(
                this.topicUri,
                properties,
                serializationSchema,
                this.partitioner);

        return SinkFunctionProvider.of(pscProducer);
    }

    /**
     * Returns the version-specific Kafka producer.
     *
     * @param topicUri            PSC topic URI to produce to.
     * @param properties          Properties for the PSC producer.
     * @param serializationSchema Serialization schema to use to create PSC messages.
     * @param partitioner         Partitioner to select PSC topic URI partition.
     * @return The version-specific Kafka producer
     */
    protected abstract SinkFunction<RowData> createPscProducer(
            String topicUri,
            Properties properties,
            SerializationSchema<RowData> serializationSchema,
            Optional<FlinkPscPartitioner<RowData>> partitioner);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PscDynamicSinkBase that = (PscDynamicSinkBase) o;
        return Objects.equals(consumedDataType, that.consumedDataType) &&
                Objects.equals(topicUri, that.topicUri) &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(encodingFormat, that.encodingFormat) &&
                Objects.equals(partitioner, that.partitioner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                consumedDataType,
                topicUri,
                properties,
                encodingFormat,
                partitioner);
    }
}
