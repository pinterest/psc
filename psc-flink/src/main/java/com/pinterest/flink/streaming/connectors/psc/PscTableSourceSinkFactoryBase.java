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

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkFixedPartitioner;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.flink.table.descriptors.psc.PscValidator;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;

/**
 * Factory for creating configured instances of {@link org.apache.flink.streaming.connectors.kafka.PscTableSourceBase}.
 */
// TODO: migration - remove

public abstract class PscTableSourceSinkFactoryBase implements
        StreamTableSourceFactory<Row>,
        StreamTableSinkFactory<Row> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, PscValidator.CONNECTOR_TYPE_VALUE_PSC); // psc
        context.put(CONNECTOR_VERSION, pscVersion()); // version
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        // update mode
        properties.add(UPDATE_MODE);

        // kafka
        properties.add(PscValidator.CONNECTOR_TOPIC);
        properties.add(PscValidator.CONNECTOR_PROPERTIES);
        properties.add(PscValidator.CONNECTOR_PROPERTIES + ".#." + PscValidator.CONNECTOR_PROPERTIES_KEY);
        properties.add(PscValidator.CONNECTOR_PROPERTIES + ".#." + PscValidator.CONNECTOR_PROPERTIES_VALUE);
        properties.add(PscValidator.CONNECTOR_PROPERTIES + ".*");
        properties.add(PscValidator.CONNECTOR_STARTUP_MODE);
        properties.add(PscValidator.CONNECTOR_SPECIFIC_OFFSETS);
        properties.add(
                PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".#." + PscValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION);
        properties.add(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".#." + PscValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET);
        properties.add(PscValidator.CONNECTOR_STARTUP_TIMESTAMP_MILLIS);
        properties.add(PscValidator.CONNECTOR_SINK_PARTITIONER);
        properties.add(PscValidator.CONNECTOR_SINK_PARTITIONER_CLASS);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        // computed column
        properties.add(SCHEMA + ".#." + EXPR);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);

        // table constraint
        properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_NAME);
        properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_COLUMNS);

        // format wildcard
        properties.add(FORMAT + ".*");

        return properties;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        final String topic = descriptorProperties.getString(PscValidator.CONNECTOR_TOPIC);
        final DeserializationSchema<Row> deserializationSchema = getDeserializationSchema(properties);
        final StartupOptions startupOptions = getStartupOptions(descriptorProperties, topic);

        return createPscTableSource(
                TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA)),
                SchemaValidator.deriveProctimeAttribute(descriptorProperties),
                SchemaValidator.deriveRowtimeAttributes(descriptorProperties),
                SchemaValidator.deriveFieldMapping(
                        descriptorProperties,
                        Optional.of(deserializationSchema.getProducedType())),
                topic,
                getPscProperties(descriptorProperties),
                deserializationSchema,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.startupTimestampMillis);
    }

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        final TableSchema schema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA));
        final String topicUri = descriptorProperties.getString(PscValidator.CONNECTOR_TOPIC);
        final Optional<String> proctime = SchemaValidator.deriveProctimeAttribute(descriptorProperties);
        final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors =
                SchemaValidator.deriveRowtimeAttributes(descriptorProperties);

        // see also FLINK-9870
        if (proctime.isPresent() || !rowtimeAttributeDescriptors.isEmpty() ||
                checkForCustomFieldMapping(descriptorProperties, schema)) {
            throw new TableException("Time attributes and custom field mappings are not supported yet.");
        }

        return createPscTableSink(
                schema,
                topicUri,
                getPscProperties(descriptorProperties),
                getFlinkPscPartitioner(descriptorProperties),
                getSerializationSchema(properties));
    }

    // --------------------------------------------------------------------------------------------
    // For version-specific factories
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the Kafka version.
     */
    protected abstract String pscVersion();

    /**
     * True if the Kafka source supports Kafka timestamps, false otherwise.
     *
     * @return True if the Kafka source supports Kafka timestamps, false otherwise.
     */
    protected abstract boolean supportsKafkaTimestamps();

    /**
     * Constructs the version-specific Kafka table source.
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
    protected abstract PscTableSourceBase createPscTableSource(
            TableSchema schema,
            Optional<String> proctimeAttribute,
            List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
            Map<String, String> fieldMapping,
            String topicUri,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema,
            StartupMode startupMode,
            Map<PscTopicUriPartition, Long> specificStartupOffsets,
            long startupTimestampMillis);

    /**
     * Constructs the version-specific Kafka table sink.
     *
     * @param schema      Schema of the produced table.
     * @param topicUri    Kafka topic to consume.
     * @param properties  Properties for the Kafka consumer.
     * @param partitioner Partitioner to select Kafka partition for each item.
     */
    protected abstract PscTableSinkBase createPscTableSink(
            TableSchema schema,
            String topicUri,
            Properties properties,
            Optional<FlinkPscPartitioner<Row>> partitioner,
            SerializationSchema<Row> serializationSchema);

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        // allow Kafka timestamps to be used, watermarks can not be received from source
        new SchemaValidator(true, supportsKafkaTimestamps(), false).validate(descriptorProperties);
        new PscValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

    private DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties) {
        @SuppressWarnings("unchecked") final DeserializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
                DeserializationSchemaFactory.class,
                properties,
                this.getClass().getClassLoader());
        return formatFactory.createDeserializationSchema(properties);
    }

    private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
        @SuppressWarnings("unchecked") final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
                SerializationSchemaFactory.class,
                properties,
                this.getClass().getClassLoader());
        return formatFactory.createSerializationSchema(properties);
    }

    private Properties getPscProperties(DescriptorProperties descriptorProperties) {
        final Properties pscProperties = new Properties();

        // we are not considering legacy configs here

        descriptorProperties.asMap().keySet()
                .stream()
                .filter(key -> key.startsWith(PscValidator.CONNECTOR_PROPERTIES))
                .forEach(key -> {
                    final String value = descriptorProperties.getString(key);
                    final String subKey = key.substring((PscValidator.CONNECTOR_PROPERTIES + '.').length());
                    pscProperties.setProperty(subKey, value);
                });
        return pscProperties;
    }

    private StartupOptions getStartupOptions(
            DescriptorProperties descriptorProperties,
            String topic) {
        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        final StartupMode startupMode = descriptorProperties
                .getOptionalString(PscValidator.CONNECTOR_STARTUP_MODE)
                .map(modeString -> {
                    switch (modeString) {
                        case PscValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST:
                            return StartupMode.EARLIEST;

                        case PscValidator.CONNECTOR_STARTUP_MODE_VALUE_LATEST:
                            return StartupMode.LATEST;

                        case PscValidator.CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS:
                            return StartupMode.GROUP_OFFSETS;

                        case PscValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
                            buildSpecificOffsets(descriptorProperties, topic, specificOffsets);
                            return StartupMode.SPECIFIC_OFFSETS;

                        case PscValidator.CONNECTOR_STARTUP_MODE_VALUE_TIMESTAMP:
                            return StartupMode.TIMESTAMP;

                        default:
                            throw new TableException("Unsupported startup mode. Validator should have checked that.");
                    }
                }).orElse(StartupMode.GROUP_OFFSETS);
        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = descriptorProperties.getLong(
                    PscValidator.CONNECTOR_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
    }

    private void buildSpecificOffsets(DescriptorProperties descriptorProperties, String topicUri, Map<PscTopicUriPartition, Long> specificOffsets) {
        if (descriptorProperties.containsKey(PscValidator.CONNECTOR_SPECIFIC_OFFSETS)) {
            final Map<Integer, Long> offsetMap = PscValidator.validateAndParseSpecificOffsetsString(descriptorProperties);
            offsetMap.forEach((partition, offset) -> {
                final PscTopicUriPartition pscTopicUriPartition = new PscTopicUriPartition(topicUri, partition);
                specificOffsets.put(pscTopicUriPartition, offset);
            });
        } else {
            final List<Map<String, String>> offsetList = descriptorProperties.getFixedIndexedProperties(
                    PscValidator.CONNECTOR_SPECIFIC_OFFSETS,
                    Arrays.asList(PscValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION, PscValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
            offsetList.forEach(kv -> {
                final int partition = descriptorProperties.getInt(kv.get(
                        PscValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION));
                final long offset = descriptorProperties.getLong(kv.get(PscValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
                final PscTopicUriPartition pscTopicUriPartition = new PscTopicUriPartition(topicUri, partition);
                specificOffsets.put(pscTopicUriPartition, offset);
            });
        }
    }

    @SuppressWarnings("unchecked")
    private Optional<FlinkPscPartitioner<Row>> getFlinkPscPartitioner(DescriptorProperties descriptorProperties) {
        return descriptorProperties
                .getOptionalString(PscValidator.CONNECTOR_SINK_PARTITIONER)
                .flatMap((String partitionerString) -> {
                    switch (partitionerString) {
                        case PscValidator.CONNECTOR_SINK_PARTITIONER_VALUE_FIXED:
                            return Optional.of(new FlinkFixedPartitioner<>());
                        case PscValidator.CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN:
                            return Optional.empty();
                        case PscValidator.CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM:
                            final Class<? extends FlinkPscPartitioner> partitionerClass =
                                    descriptorProperties.getClass(PscValidator.CONNECTOR_SINK_PARTITIONER_CLASS, FlinkPscPartitioner.class);
                            return Optional.of((FlinkPscPartitioner<Row>) InstantiationUtil.instantiate(partitionerClass));
                        default:
                            throw new TableException("Unsupported sink partitioner. Validator should have checked that.");
                    }
                });
    }

    private boolean checkForCustomFieldMapping(DescriptorProperties descriptorProperties, TableSchema schema) {
        final Map<String, String> fieldMapping = SchemaValidator.deriveFieldMapping(
                descriptorProperties,
                Optional.of(schema.toRowType())); // until FLINK-9870 is fixed we assume that the table schema is the output type
        return fieldMapping.size() != schema.getFieldNames().length ||
                !fieldMapping.entrySet().stream().allMatch(mapping -> mapping.getKey().equals(mapping.getValue()));
    }

    private static class StartupOptions {
        private StartupMode startupMode;
        private Map<PscTopicUriPartition, Long> specificOffsets;
        private long startupTimestampMillis;
    }
}
