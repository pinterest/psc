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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Kafka {@link StreamTableSource}.
 */
// TODO: migration - remove

@Internal
public class PscTableSource extends PscTableSourceBase {

    /**
     * Creates a generic Kafka {@link StreamTableSource}.
     *
     * @param schema                      Schema of the produced table.
     * @param proctimeAttribute           Field name of the processing time attribute.
     * @param rowtimeAttributeDescriptors Descriptor for a rowtime attribute
     * @param fieldMapping                Mapping for the fields of the table schema to
     *                                    fields of the physical returned type.
     * @param topicUri                    Kafka topic to consume.
     * @param properties                  Properties for the Kafka consumer.
     * @param deserializationSchema       Deserialization schema for decoding records from Kafka.
     * @param startupMode                 Startup mode for the contained consumer.
     * @param specificStartupOffsets      Specific startup offsets; only relevant when startup
     *                                    mode is {@link StartupMode#SPECIFIC_OFFSETS}.
     */
    public PscTableSource(
            TableSchema schema,
            Optional<String> proctimeAttribute,
            List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
            Optional<Map<String, String>> fieldMapping,
            String topicUri,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema,
            StartupMode startupMode,
            Map<PscTopicUriPartition, Long> specificStartupOffsets,
            long startupTimestampMillis) {

        super(
                schema,
                proctimeAttribute,
                rowtimeAttributeDescriptors,
                fieldMapping,
                topicUri,
                properties,
                deserializationSchema,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis);
    }

    /**
     * Creates a generic PSC {@link StreamTableSource}.
     *
     * @param schema                Schema of the produced table.
     * @param topicUri              Kafka topic to consume.
     * @param properties            Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema for decoding records from Kafka.
     */
    public PscTableSource(
            TableSchema schema,
            String topicUri,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema) {

        super(schema, topicUri, properties, deserializationSchema);
    }

    @Override
    protected FlinkPscConsumerBase<Row> createPscConsumer(
            String topicUri,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema) {

        return new FlinkPscConsumer<>(topicUri, deserializationSchema, properties);
    }
}
