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

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.flink.table.descriptors.psc.PscValidator;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Test for {@link PscTableSource} and {@link PscTableSink} created
 * by {@link PscTableSourceSinkFactory}.
 */
public class PscTableSourceSinkFactoryTest extends PscTableSourceSinkFactoryTestBase {

    @Override
    protected String getPscVersion() {
        return PscValidator.CONNECTOR_VERSION_VALUE_UNIVERSAL;
    }

    @Override
    protected Class<FlinkPscConsumerBase<Row>> getExpectedFlinkPscConsumer() {
        return (Class) FlinkPscConsumer.class;
    }

    @Override
    protected Class<?> getExpectedFlinkPscProducer() {
        return FlinkPscProducer.class;
    }

    @Override
    protected PscTableSourceBase getExpectedPscTableSource(
            TableSchema schema,
            Optional<String> proctimeAttribute,
            List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
            Map<String, String> fieldMapping,
            String topicUri,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema,
            StartupMode startupMode,
            Map<PscTopicUriPartition, Long> specificStartupOffsets,
            long startupTimestamp) {

        return new PscTableSource(
                schema,
                proctimeAttribute,
                rowtimeAttributeDescriptors,
                Optional.of(fieldMapping),
                topicUri,
                properties,
                deserializationSchema,
                startupMode,
                specificStartupOffsets,
                startupTimestamp);
    }

    protected PscTableSinkBase getExpectedPscTableSink(
            TableSchema schema,
            String topicUri,
            Properties properties,
            Optional<FlinkPscPartitioner<Row>> partitioner,
            SerializationSchema<Row> serializationSchema) {

        return new PscTableSink(
                schema,
                topicUri,
                properties,
                partitioner,
                serializationSchema);
    }
}
