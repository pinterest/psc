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

import com.pinterest.flink.streaming.connectors.psc.FlinkPscConsumerBase;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import com.pinterest.flink.streaming.connectors.psc.FlinkPscConsumer;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;

import java.util.Map;
import java.util.Properties;

/**
 * Kafka {@link DynamicTableSource}.
 */
@Internal
public class PscDynamicSource extends PscDynamicSourceBase {

    /**
     * Creates a generic Kafka {@link StreamTableSource}.
     *
     * @param outputDataType         Source output data type
     * @param topicUri               Kafka topic to consume
     * @param properties             Properties for the Kafka consumer
     * @param decodingFormat         Decoding format for decoding records from Kafka
     * @param startupMode            Startup mode for the contained consumer
     * @param specificStartupOffsets Specific startup offsets; only relevant when startup
     *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}
     */
    public PscDynamicSource(
            DataType outputDataType,
            String topicUri,
            Properties properties,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            StartupMode startupMode,
            Map<PscTopicUriPartition, Long> specificStartupOffsets,
            long startupTimestampMillis) {

        super(
                outputDataType,
                topicUri,
                properties,
                decodingFormat,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis);
    }

    @Override
    protected FlinkPscConsumerBase<RowData> createPscConsumer(
            String topicUri,
            Properties properties,
            DeserializationSchema<RowData> deserializationSchema) {
        return new FlinkPscConsumer<>(topicUri, deserializationSchema, properties);
    }

    @Override
    public DynamicTableSource copy() {
        return new PscDynamicSource(
                this.outputDataType,
                this.topicUri,
                this.properties,
                this.decodingFormat,
                this.startupMode,
                this.specificStartupOffsets,
                this.startupTimestampMillis);
    }

    @Override
    public String asSummaryString() {
        return "PSC";
    }
}
