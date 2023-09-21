/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc.table;

import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.PROPS_GROUP_ID;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.SCAN_STARTUP_MODE;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.SINK_PARTITIONER;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.TOPIC_URI;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.getFlinkPscPartitioner;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.getPscProperties;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.getStartupOptions;
import static com.pinterest.flink.streaming.connectors.psc.table.PscOptions.validateTableOptions;

/**
 * Factory for creating configured instances of
 * {@link PscDynamicSourceBase} and {@link PscDynamicSinkBase}.
 */
public abstract class PscDynamicTableFactoryBase implements
        DynamicTableSourceFactory,
        DynamicTableSinkFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        String topicUri = tableOptions.get(TOPIC_URI);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(PscOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        validateTableOptions(tableOptions);

        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        final PscOptions.StartupOptions startupOptions = getStartupOptions(tableOptions, topicUri);
        return createPscTableSource(
                producedDataType,
                topicUri,
                getPscProperties(context.getCatalogTable().getOptions()),
                decodingFormat,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.startupTimestampMillis);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        String topic = tableOptions.get(TOPIC_URI);
        EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);
        // Validate the option data type.
        helper.validateExcept(PscOptions.PROPERTIES_PREFIX);
        // Validate the option values.
        validateTableOptions(tableOptions);

        DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return createPscTableSink(
                consumedDataType,
                topic,
                getPscProperties(context.getCatalogTable().getOptions()),
                getFlinkPscPartitioner(tableOptions, context.getClassLoader()),
                encodingFormat);
    }

    /**
     * Constructs the version-specific Kafka table source.
     *
     * @param producedDataType       Source produced data type
     * @param topicUri               Kafka topic to consume
     * @param properties             Properties for the Kafka consumer
     * @param decodingFormat         Decoding format for decoding records from Kafka
     * @param startupMode            Startup mode for the contained consumer
     * @param specificStartupOffsets Specific startup offsets; only relevant when startup
     *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}
     */
    protected abstract PscDynamicSourceBase createPscTableSource(
            DataType producedDataType,
            String topicUri,
            Properties properties,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            StartupMode startupMode,
            Map<PscTopicUriPartition, Long> specificStartupOffsets,
            long startupTimestampMillis);

    /**
     * Constructs the version-specific Kafka table sink.
     *
     * @param consumedDataType Sink consumed data type
     * @param topicUri         Kafka topic to consume
     * @param properties       Properties for the Kafka consumer
     * @param partitioner      Partitioner to select Kafka partition for each item
     * @param encodingFormat   Encoding format for encoding records to Kafka
     */
    protected abstract PscDynamicSinkBase createPscTableSink(
            DataType consumedDataType,
            String topicUri,
            Properties properties,
            Optional<FlinkPscPartitioner<RowData>> partitioner,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat);

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TOPIC_URI);
        options.add(FactoryUtil.FORMAT);
        //options.add(PROPS_BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPS_GROUP_ID);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SINK_PARTITIONER);
        return options;
    }
}
