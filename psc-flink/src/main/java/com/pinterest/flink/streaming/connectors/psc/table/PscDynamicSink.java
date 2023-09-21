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

package com.pinterest.flink.streaming.connectors.psc.table;

import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.pinterest.flink.streaming.connectors.psc.FlinkPscProducer;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Optional;
import java.util.Properties;

/**
 * PSC table sink for writing data using PSC Producer.
 */
@Internal
public class PscDynamicSink extends PscDynamicSinkBase {

    public PscDynamicSink(
            DataType consumedDataType,
            String topicUri,
            Properties properties,
            Optional<FlinkPscPartitioner<RowData>> partitioner,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        super(
                consumedDataType,
                topicUri,
                properties,
                partitioner,
                encodingFormat);
    }

    @Override
    protected SinkFunction<RowData> createPscProducer(
            String topicUri,
            Properties properties,
            SerializationSchema<RowData> serializationSchema,
            Optional<FlinkPscPartitioner<RowData>> partitioner) {
        return new FlinkPscProducer<>(
                topicUri,
                serializationSchema,
                properties,
                partitioner);
    }

    @Override
    public DynamicTableSink copy() {
        return new PscDynamicSink(
                this.consumedDataType,
                this.topicUri,
                this.properties,
                this.partitioner,
                this.encodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "PSC universal table sink";
    }
}
