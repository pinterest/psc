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

import com.pinterest.flink.streaming.connectors.psc.PscTableSink;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import com.pinterest.flink.streaming.connectors.psc.FlinkPscConsumer;
import com.pinterest.flink.streaming.connectors.psc.FlinkPscProducer;
import com.pinterest.flink.streaming.connectors.psc.PscTableSource;
import com.pinterest.flink.streaming.connectors.psc.PscTableSourceSinkFactory;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Test for {@link PscTableSource} and {@link PscTableSink} created
 * by {@link PscTableSourceSinkFactory}.
 */
public class PscDynamicTableFactoryTest extends PscDynamicTableFactoryTestBase {
    @Override
    protected String factoryIdentifier() {
        return PscDynamicTableFactory.IDENTIFIER;
    }

    @Override
    protected Class<?> getExpectedConsumerClass() {
        return FlinkPscConsumer.class;
    }

    @Override
    protected Class<?> getExpectedProducerClass() {
        return FlinkPscProducer.class;
    }

    @Override
    protected PscDynamicSourceBase getExpectedScanSource(
            DataType producedDataType,
            String topic,
            Properties properties,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            StartupMode startupMode,
            Map<PscTopicUriPartition, Long> specificStartupOffsets,
            long startupTimestamp) {
        return new PscDynamicSource(
                producedDataType,
                topic,
                properties,
                decodingFormat,
                startupMode,
                specificStartupOffsets,
                startupTimestamp);
    }

    @Override
    protected PscDynamicSinkBase getExpectedSink(
            DataType consumedDataType,
            String topic,
            Properties properties,
            Optional<FlinkPscPartitioner<RowData>> partitioner,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        return new PscDynamicSink(
                consumedDataType,
                topic,
                properties,
                partitioner,
                encodingFormat);
    }
}
