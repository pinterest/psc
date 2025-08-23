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

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.sink.PscSink;
import com.pinterest.flink.connector.psc.source.PscSource;
import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.PscSourceTestUtils;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumState;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.config.BoundedMode;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkFixedPartitioner;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.flink.streaming.connectors.psc.testutils.MockPartitionOffsetsRetriever;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.factories.TestFormatFactory.DecodingFormatMock;
import org.apache.flink.table.factories.TestFormatFactory.EncodingFormatMock;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLoggerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptionsUtil.AVRO_CONFLUENT;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptionsUtil.DEBEZIUM_AVRO_CONFLUENT;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link PscDynamicTableFactory}. */
@ExtendWith(TestLoggerExtension.class)
public class PscDynamicTableFactoryTest {

    private static final String TOPIC = "myTopic";
    private static final String TOPIC_URI = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + TOPIC;
    private static final String TOPICS = "myTopic-1;myTopic-2;myTopic-3";
    private static final String TOPIC_URIS = Arrays.stream(TOPICS.split(";")).map(topic -> PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic).reduce((a, b) -> a + ";" + b).get();
    private static final String TOPIC_REGEX = "myTopic-\\d+";
    private static final List<String> TOPIC_LIST =
            Arrays.asList("myTopic-1", "myTopic-2", "myTopic-3");
    private static final List<String> TOPIC_URI_LIST = TOPIC_LIST.stream().map(topic -> PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic).collect(Collectors.toList());
    private static final String TEST_REGISTRY_URL = "http://localhost:8081";
    private static final String DEFAULT_VALUE_SUBJECT = TOPIC + "-value";
    private static final String DEFAULT_KEY_SUBJECT = TOPIC + "-key";
    private static final int PARTITION_0 = 0;
    private static final long OFFSET_0 = 100L;
    private static final int PARTITION_1 = 1;
    private static final long OFFSET_1 = 123L;
    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String METADATA = "metadata";
    private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);
    private static final String DISCOVERY_INTERVAL = "1000 ms";

    private static final Properties PSC_SOURCE_PROPERTIES = new Properties();
    private static final Properties PSC_KV_SOURCE_PROPERTIES = new Properties();
    private static final Properties PSC_SINK_PROPERTIES = new Properties();
    private static final Properties PSC_KV_SINK_PROPERTIES = new Properties();

    static {
        PSC_SOURCE_PROPERTIES.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        PSC_SOURCE_PROPERTIES.setProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        PSC_SOURCE_PROPERTIES.setProperty("client.id.prefix", "test");
        PSC_SOURCE_PROPERTIES.setProperty(PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "1000");

        PSC_SINK_PROPERTIES.setProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        PSC_SINK_PROPERTIES.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "test-producer");

        PSC_KV_SINK_PROPERTIES.putAll(PSC_SINK_PROPERTIES);
        PSC_KV_SOURCE_PROPERTIES.putAll(PSC_SOURCE_PROPERTIES);
        // Add client.id.prefix to key-value sink properties for key-value sink tests
        PSC_KV_SINK_PROPERTIES.setProperty("client.id.prefix", "test");
    }

    private static final String PROPS_SCAN_OFFSETS =
            String.format(
                    "partition:%d,offset:%d;partition:%d,offset:%d",
                    PARTITION_0, OFFSET_0, PARTITION_1, OFFSET_1);

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(NAME, DataTypes.STRING().notNull()),
                            Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
                            Column.physical(TIME, DataTypes.TIMESTAMP(3)),
                            Column.computed(
                                    COMPUTED_COLUMN_NAME,
                                    ResolvedExpressionMock.of(
                                            COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION))),
                    Collections.singletonList(
                            WatermarkSpec.of(
                                    TIME,
                                    ResolvedExpressionMock.of(
                                            WATERMARK_DATATYPE, WATERMARK_EXPRESSION))),
                    null);

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(NAME, DataTypes.STRING()),
                            Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
                            Column.metadata(TIME, DataTypes.TIMESTAMP(3), "timestamp", false),
                            Column.metadata(
                                    METADATA, DataTypes.STRING(), "value.metadata_2", false)),
                    Collections.emptyList(),
                    null);

    private static final DataType SCHEMA_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

    @Test
    public void testTableSource() {
        final DynamicTableSource actualSource = createTableSource(SCHEMA, getBasicSourceOptions());
        final PscDynamicSource actualPscSource = (PscDynamicSource) actualSource;

        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_0), OFFSET_0);
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_1), OFFSET_1);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);

        // Test scan source equals
        final PscDynamicSource expectedPscSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        Collections.singletonList(TOPIC_URI),
                        null,
                        PSC_SOURCE_PROPERTIES,
                        StartupMode.SPECIFIC_OFFSETS,
                        specificOffsets,
                        0);
        assertThat(actualPscSource).isEqualTo(expectedPscSource);

        ScanTableSource.ScanRuntimeProvider provider =
                actualPscSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertPscSource(provider);
    }

    @Test
    public void testTableSourceWithPattern() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.remove("topic-uri");
                            options.put("topic-pattern", TOPIC_REGEX);
                            options.put(
                                    "scan.startup.mode",
                                    PscConnectorOptions.ScanStartupMode.EARLIEST_OFFSET.toString());
                            options.remove("scan.startup.specific-offsets");
                        });
        final DynamicTableSource actualSource = createTableSource(SCHEMA, modifiedOptions);

        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();

        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);

        // Test scan source equals
        final PscDynamicSource expectedPscSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        null,
                        Pattern.compile(TOPIC_REGEX),
                        PSC_SOURCE_PROPERTIES,
                        StartupMode.EARLIEST,
                        specificOffsets,
                        0);
        final PscDynamicSource actualPscSource = (PscDynamicSource) actualSource;
        assertThat(actualPscSource).isEqualTo(expectedPscSource);

        ScanTableSource.ScanRuntimeProvider provider =
                actualPscSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertPscSource(provider);
    }

    @Test
    public void testTableSourceWithKeyValue() {
        final DynamicTableSource actualSource = createTableSource(SCHEMA, getKeyValueSourceOptions());
        final PscDynamicSource actualPscSource = (PscDynamicSource) actualSource;
        // initialize stateful testing formats
        actualPscSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        final DecodingFormatMock keyDecodingFormat = new DecodingFormatMock("#", false);
        keyDecodingFormat.producedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING().notNull())).notNull();

        final DecodingFormatMock valueDecodingFormat = new DecodingFormatMock("|", false);
        valueDecodingFormat.producedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD(TIME, DataTypes.TIMESTAMP(3)))
                        .notNull();

        final PscDynamicSource expectedPscSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        new int[] {0},
                        new int[] {1, 2},
                        null,
                        Collections.singletonList(TOPIC_URI),
                        null,
                        PSC_KV_SOURCE_PROPERTIES,
                        StartupMode.GROUP_OFFSETS,
                        Collections.emptyMap(),
                        0);

        assertThat(actualSource).isEqualTo(expectedPscSource);
    }

    @Test
    public void testTableSourceWithKeyValueAndMetadata() {
        final Map<String, String> options = getKeyValueSourceOptions();
        options.put("value.test-format.readable-metadata", "metadata_1:INT, metadata_2:STRING");

        final DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, options);
        final PscDynamicSource actualPscSource = (PscDynamicSource) actualSource;
        // initialize stateful testing formats
        actualPscSource.applyReadableMetadata(
                Arrays.asList("timestamp", "value.metadata_2"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualPscSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        final DecodingFormatMock expectedKeyFormat =
                new DecodingFormatMock(
                        "#", false, ChangelogMode.insertOnly(), Collections.emptyMap());
        expectedKeyFormat.producedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING())).notNull();

        final Map<String, DataType> expectedReadableMetadata = new HashMap<>();
        expectedReadableMetadata.put("metadata_1", DataTypes.INT());
        expectedReadableMetadata.put("metadata_2", DataTypes.STRING());

        final DecodingFormatMock expectedValueFormat =
                new DecodingFormatMock(
                        "|", false, ChangelogMode.insertOnly(), expectedReadableMetadata);
        expectedValueFormat.producedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD("metadata_2", DataTypes.STRING()))
                        .notNull();
        expectedValueFormat.metadataKeys = Collections.singletonList("metadata_2");

        final PscDynamicSource expectedPscSource =
                createExpectedScanSource(
                        SCHEMA_WITH_METADATA.toPhysicalRowDataType(),
                        expectedKeyFormat,
                        expectedValueFormat,
                        new int[] {0},
                        new int[] {1},
                        null,
                        Collections.singletonList(TOPIC_URI),
                        null,
                        PSC_KV_SOURCE_PROPERTIES,
                        StartupMode.GROUP_OFFSETS,
                        Collections.emptyMap(),
                        0);
        expectedPscSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedPscSource.metadataKeys = Collections.singletonList("timestamp");

        assertThat(actualSource).isEqualTo(expectedPscSource);
    }

    @Test
    public void testTableSourceWithAutoGenUuidGroupId() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
                        });

        // Test the AUTO_GEN_UUID processing directly using the utility method
        Properties processedProperties = PscConnectorOptionsUtil.getPscProperties(modifiedOptions);
        String groupId = processedProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        // Verify AUTO_GEN_UUID was processed correctly
        assertThat(groupId).isNotNull();
        assertThat(groupId).isNotEqualTo("AUTO_GEN_UUID");
        assertThat(groupId).startsWith("test-"); // Should start with client.id.prefix from getBasicSourceOptions()

        // Verify the suffix after prefix is a valid UUID
        String uuidPart = groupId.substring("test-".length());
        try {
            java.util.UUID.fromString(uuidPart);
            // If no exception is thrown, the UUID is valid
        } catch (IllegalArgumentException e) {
            fail("Generated group ID suffix should be a valid UUID: " + uuidPart);
        }

        // Verify other properties remain unchanged
        assertThat(processedProperties.getProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG))
                .isEqualTo(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        assertThat(processedProperties.getProperty("client.id.prefix")).isEqualTo("test");

        // Verify that the table source can be created successfully with AUTO_GEN_UUID
        final DynamicTableSource actualSource = createTableSource(SCHEMA, modifiedOptions);
        assertThat(actualSource).isInstanceOf(PscDynamicSource.class);

        // Verify the scan runtime provider works correctly
        final PscDynamicSource actualPscSource = (PscDynamicSource) actualSource;
        ScanTableSource.ScanRuntimeProvider provider =
                actualPscSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertPscSource(provider);
    }

    @Test
    public void testTableSourceWithAutoGenUuidGeneratesDifferentGroupIds() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
                        });

        // Generate multiple group IDs using the same options
        Properties properties1 = PscConnectorOptionsUtil.getPscProperties(modifiedOptions);
        Properties properties2 = PscConnectorOptionsUtil.getPscProperties(modifiedOptions);
        Properties properties3 = PscConnectorOptionsUtil.getPscProperties(modifiedOptions);

        String groupId1 = properties1.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String groupId2 = properties2.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String groupId3 = properties3.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        // Verify all group IDs are different
        assertThat(groupId1).isNotEqualTo(groupId2);
        assertThat(groupId1).isNotEqualTo(groupId3);
        assertThat(groupId2).isNotEqualTo(groupId3);

        // Verify all group IDs have correct format (prefix + UUID)
        assertThat(groupId1).startsWith("test-");
        assertThat(groupId2).startsWith("test-");
        assertThat(groupId3).startsWith("test-");

        // Verify all UUIDs are valid
        String uuid1 = groupId1.substring("test-".length());
        String uuid2 = groupId2.substring("test-".length());
        String uuid3 = groupId3.substring("test-".length());

        try {
            java.util.UUID.fromString(uuid1);
            java.util.UUID.fromString(uuid2);
            java.util.UUID.fromString(uuid3);
            // If no exception is thrown, all UUIDs are valid
        } catch (IllegalArgumentException e) {
            fail("All generated UUIDs should be valid: " + e.getMessage());
        }

        // Also verify that table sources can be created successfully with these different group IDs
        final DynamicTableSource tableSource1 = createTableSource(SCHEMA, modifiedOptions);
        final DynamicTableSource tableSource2 = createTableSource(SCHEMA, modifiedOptions);

        assertThat(tableSource1).isInstanceOf(PscDynamicSource.class);
        assertThat(tableSource2).isInstanceOf(PscDynamicSource.class);

        // The table sources should be functionally equivalent but have different internal group IDs
        // (we can't directly access the properties, but the creation should succeed)
    }

    @Test
    public void testTableSourceWithAutoGenUuidGroupIdFailsWithMissingClientIdPrefix() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
                            options.remove("properties.client.id.prefix"); // Remove client.id.prefix
                        });

        assertThatThrownBy(() -> createTableSource(SCHEMA, modifiedOptions))
                .isInstanceOf(ValidationException.class)
                .hasCauseInstanceOf(ValidationException.class)
                .satisfies(exception ->
                    assertThat(exception.getCause().getMessage())
                        .contains("properties.client.id.prefix must be provided as it is mandatory for PSC table sources"));
    }

    @Test
    public void testTableSourceWithAutoGenUuidGroupIdFailsWithEmptyClientIdPrefix() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
                            options.put("properties.client.id.prefix", ""); // Empty client.id.prefix
                        });

        assertThatThrownBy(() -> createTableSource(SCHEMA, modifiedOptions))
                .isInstanceOf(ValidationException.class)
                .hasCauseInstanceOf(ValidationException.class)
                .satisfies(exception ->
                    assertThat(exception.getCause().getMessage())
                        .contains("properties.client.id.prefix must be non-empty (after trimming whitespace) as it is mandatory for PSC table sources"));
    }

    @Test
    public void testTableSourceWithAutoGenUuidGroupIdFailsWithBlankClientIdPrefix() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
                            options.put("properties.client.id.prefix", "   "); // Blank spaces client.id.prefix
                        });

        assertThatThrownBy(() -> createTableSource(SCHEMA, modifiedOptions))
                .isInstanceOf(ValidationException.class)
                .hasCauseInstanceOf(ValidationException.class)
                .satisfies(exception ->
                    assertThat(exception.getCause().getMessage())
                        .contains("properties.client.id.prefix must be non-empty (after trimming whitespace) as it is mandatory for PSC table sources"));
    }

    @Test
    public void testTableSinkWithAutoGenUuidProducerId() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> {
                            options.put("properties.client.id.prefix", "test");
                            options.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "AUTO_GEN_UUID");
                        });

        // Test the AUTO_GEN_UUID processing directly using the utility method
        Properties processedProperties = PscConnectorOptionsUtil.getPscProperties(modifiedOptions);
        String producerId = processedProperties.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);

        // Verify AUTO_GEN_UUID was processed correctly
        assertThat(producerId).isNotNull();
        assertThat(producerId).isNotEqualTo("AUTO_GEN_UUID");
        assertThat(producerId).startsWith("test-"); // Should start with client.id.prefix from getBasicSinkOptions()

        // Verify the suffix after prefix is a valid UUID
        String uuidPart = producerId.substring("test-".length());
        try {
            java.util.UUID.fromString(uuidPart);
            // If no exception is thrown, the UUID is valid
        } catch (IllegalArgumentException e) {
            fail("Generated producer ID suffix should be a valid UUID: " + uuidPart);
        }

        // Verify other properties remain unchanged
        assertThat(processedProperties.getProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG))
                .isEqualTo(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        assertThat(processedProperties.getProperty("client.id.prefix")).isEqualTo("test");

        // Verify that the table sink can be created successfully with AUTO_GEN_UUID
        final DynamicTableSink actualSink = createTableSink(SCHEMA, modifiedOptions);
        assertThat(actualSink).isInstanceOf(PscDynamicSink.class);

        // Verify the sink runtime provider works correctly
        final PscDynamicSink actualPscSink = (PscDynamicSink) actualSink;
        DynamicTableSink.SinkRuntimeProvider provider =
                actualPscSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider).isInstanceOf(SinkV2Provider.class);
    }

    @Test
    public void testTableSinkWithAutoGenUuidGeneratesDifferentProducerIds() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> {
                            options.put("properties.client.id.prefix", "test");
                            options.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "AUTO_GEN_UUID");
                        });

        // Generate multiple producer IDs using the same options
        Properties properties1 = PscConnectorOptionsUtil.getPscProperties(modifiedOptions);
        Properties properties2 = PscConnectorOptionsUtil.getPscProperties(modifiedOptions);
        Properties properties3 = PscConnectorOptionsUtil.getPscProperties(modifiedOptions);

        String producerId1 = properties1.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);
        String producerId2 = properties2.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);
        String producerId3 = properties3.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);

        // Verify all producer IDs are different
        assertThat(producerId1).isNotEqualTo(producerId2);
        assertThat(producerId1).isNotEqualTo(producerId3);
        assertThat(producerId2).isNotEqualTo(producerId3);

        // Verify all producer IDs have correct format (prefix + UUID)
        assertThat(producerId1).startsWith("test-");
        assertThat(producerId2).startsWith("test-");
        assertThat(producerId3).startsWith("test-");

        // Verify all UUIDs are valid
        String uuid1 = producerId1.substring("test-".length());
        String uuid2 = producerId2.substring("test-".length());
        String uuid3 = producerId3.substring("test-".length());

        try {
            java.util.UUID.fromString(uuid1);
            java.util.UUID.fromString(uuid2);
            java.util.UUID.fromString(uuid3);
            // If no exception is thrown, all UUIDs are valid
        } catch (IllegalArgumentException e) {
            fail("All generated UUIDs should be valid: " + e.getMessage());
        }

        // Also verify that table sinks can be created successfully with these different producer IDs
        final DynamicTableSink tableSink1 = createTableSink(SCHEMA, modifiedOptions);
        final DynamicTableSink tableSink2 = createTableSink(SCHEMA, modifiedOptions);

        assertThat(tableSink1).isInstanceOf(PscDynamicSink.class);
        assertThat(tableSink2).isInstanceOf(PscDynamicSink.class);

        // The table sinks should be functionally equivalent but have different internal producer IDs
        // (we can't directly access the properties, but the creation should succeed)
    }

    @Test
    public void testTableSinkWithAutoGenUuidProducerIdFailsWithMissingClientIdPrefix() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> {
                            options.put("properties.client.id.prefix", "test"); // Add first, then remove
                            options.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "AUTO_GEN_UUID");
                            options.remove("properties.client.id.prefix"); // Remove client.id.prefix
                        });

        assertThatThrownBy(() -> createTableSink(SCHEMA, modifiedOptions))
                .isInstanceOf(ValidationException.class)
                .hasCauseInstanceOf(ValidationException.class)
                .satisfies(exception ->
                    assertThat(exception.getCause().getMessage())
                        .contains("properties.client.id.prefix must be provided as it is mandatory for PSC table sources"));
    }

    @Test
    public void testTableSinkWithAutoGenUuidProducerIdFailsWithEmptyClientIdPrefix() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> {
                            options.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "AUTO_GEN_UUID");
                            options.put("properties.client.id.prefix", ""); // Empty client.id.prefix
                        });

        assertThatThrownBy(() -> createTableSink(SCHEMA, modifiedOptions))
                .isInstanceOf(ValidationException.class)
                .hasCauseInstanceOf(ValidationException.class)
                .satisfies(exception ->
                    assertThat(exception.getCause().getMessage())
                        .contains("properties.client.id.prefix must be non-empty (after trimming whitespace) as it is mandatory for PSC table sources"));
    }

    @Test
    public void testTableSinkWithAutoGenUuidProducerIdFailsWithBlankClientIdPrefix() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> {
                            options.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "AUTO_GEN_UUID");
                            options.put("properties.client.id.prefix", "   "); // Blank spaces client.id.prefix
                        });

        assertThatThrownBy(() -> createTableSink(SCHEMA, modifiedOptions))
                .isInstanceOf(ValidationException.class)
                .hasCauseInstanceOf(ValidationException.class)
                .satisfies(exception ->
                    assertThat(exception.getCause().getMessage())
                        .contains("properties.client.id.prefix must be non-empty (after trimming whitespace) as it is mandatory for PSC table sources"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "earliest", "latest"})
    @NullSource
    public void testTableSourceSetOffsetReset(final String strategyName) {
        testSetOffsetResetForStartFromGroupOffsets(strategyName);
    }

    @Test
    public void testTableSourceSetOffsetResetWithException() {
        String errorStrategy = "errorStrategy";
        assertThatThrownBy(() -> testTableSourceSetOffsetReset(errorStrategy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        String.format(
                                "%s can not be set to %s. Valid values: [earliest,latest,none]",
                                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, errorStrategy));
    }

    private void testSetOffsetResetForStartFromGroupOffsets(String value) {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.remove("scan.startup.mode");
                            if (value == null) {
                                return;
                            }
                            options.put(
                                    PROPERTIES_PREFIX + PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET,
                                    value);
                        });
        final DynamicTableSource tableSource = createTableSource(SCHEMA, modifiedOptions);
        assertThat(tableSource).isInstanceOf(PscDynamicSource.class);
        ScanTableSource.ScanRuntimeProvider provider =
                ((PscDynamicSource) tableSource)
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(provider).isInstanceOf(DataStreamScanProvider.class);
        final PscSource<?> pscSource = assertPscSource(provider);
        final Configuration configuration =
                PscSourceTestUtils.getPscSourceConfiguration(pscSource);

        if (value == null) {
            assertThat(configuration.toMap().get(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET))
                    .isEqualTo("none");
        } else {
            assertThat(configuration.toMap().get(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET))
                    .isEqualTo(value);
        }
    }

    @Test
    public void testBoundedSpecificOffsetsValidate() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.put(
                                    PscConnectorOptions.SCAN_BOUNDED_MODE.key(),
                                    "specific-offsets");
                        });
        assertThatThrownBy(() -> createTableSource(SCHEMA, modifiedOptions))
                .cause()
                .hasMessageContaining(
                        "'scan.bounded.specific-offsets' is required in 'specific-offsets' bounded mode but missing.");
    }

    @Test
    public void testBoundedSpecificOffsets() {
        testBoundedOffsets(
                "specific-offsets",
                options -> {
                    options.put("scan.bounded.specific-offsets", "partition:0,offset:2");
                },
                source -> {
                    assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
                    OffsetsInitializer offsetsInitializer =
                            PscSourceTestUtils.getStoppingOffsetsInitializer(source);
                    TopicUriPartition partition = new TopicUriPartition(TOPIC_URI, 0);
                    Map<TopicUriPartition, Long> partitionOffsets =
                            offsetsInitializer.getPartitionOffsets(
                                    Collections.singletonList(partition),
                                    MockPartitionOffsetsRetriever.noInteractions());
                    assertThat(partitionOffsets)
                            .containsOnlyKeys(partition)
                            .containsEntry(partition, 2L);
                });
    }

    @Test
    public void testBoundedLatestOffset() {
        testBoundedOffsets(
                "latest-offset",
                options -> {},
                source -> {
                    assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
                    OffsetsInitializer offsetsInitializer =
                            PscSourceTestUtils.getStoppingOffsetsInitializer(source);
                    TopicUriPartition partition = new TopicUriPartition(TOPIC, 0);
                    long endOffsets = 123L;
                    Map<TopicUriPartition, Long> partitionOffsets =
                            offsetsInitializer.getPartitionOffsets(
                                    Collections.singletonList(partition),
                                    MockPartitionOffsetsRetriever.latest(
                                            (tps) ->
                                                    Collections.singletonMap(
                                                            partition, endOffsets)));
                    assertThat(partitionOffsets)
                            .containsOnlyKeys(partition)
                            .containsEntry(partition, endOffsets);
                });
    }

    @Test
    public void testBoundedGroupOffsets() {
        testBoundedOffsets(
                "group-offsets",
                options -> {},
                source -> {
                    assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
                    OffsetsInitializer offsetsInitializer =
                            PscSourceTestUtils.getStoppingOffsetsInitializer(source);
                    TopicUriPartition partition = new TopicUriPartition(TOPIC, 0);
                    Map<TopicUriPartition, Long> partitionOffsets =
                            offsetsInitializer.getPartitionOffsets(
                                    Collections.singletonList(partition),
                                    MockPartitionOffsetsRetriever.noInteractions());
                    assertThat(partitionOffsets)
                            .containsOnlyKeys(partition)
                            .containsEntry(partition, PscTopicUriPartitionSplit.COMMITTED_OFFSET);
                });
    }

    @Test
    public void testBoundedTimestamp() {
        testBoundedOffsets(
                "timestamp",
                options -> {
                    options.put("scan.bounded.timestamp-millis", "1");
                },
                source -> {
                    assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
                    OffsetsInitializer offsetsInitializer =
                            PscSourceTestUtils.getStoppingOffsetsInitializer(source);
                    TopicUriPartition partition = new TopicUriPartition(TOPIC_URI, 0);
                    long offsetForTimestamp = 123L;
                    Map<TopicUriPartition, Long> partitionOffsets =
                            offsetsInitializer.getPartitionOffsets(
                                    Collections.singletonList(partition),
                                    MockPartitionOffsetsRetriever.timestampAndEnd(
                                            partitions -> {
                                                assertThat(partitions)
                                                        .containsOnlyKeys(partition)
                                                        .containsEntry(partition, 1L);
                                                Map<TopicUriPartition, Long> result =
                                                        new HashMap<>();
                                                result.put(
                                                        partition,
                                                        123L);
                                                return result;
                                            },
                                            partitions -> {
                                                Map<TopicUriPartition, Long> result = new HashMap<>();
                                                result.put(
                                                        partition,
                                                        // the end offset is bigger than given by
                                                        // timestamp
                                                        // to make sure the one for timestamp is
                                                        // used
                                                        offsetForTimestamp + 1000L);
                                                return result;
                                            }));
                    assertThat(partitionOffsets)
                            .containsOnlyKeys(partition)
                            .containsEntry(partition, offsetForTimestamp);
                });
    }

    private void testBoundedOffsets(
            String boundedMode,
            Consumer<Map<String, String>> optionsConfig,
            Consumer<PscSource<?>> validator) {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.put(PscConnectorOptions.SCAN_BOUNDED_MODE.key(), boundedMode);
                            optionsConfig.accept(options);
                        });
        final DynamicTableSource tableSource = createTableSource(SCHEMA, modifiedOptions);
        assertThat(tableSource).isInstanceOf(PscDynamicSource.class);
        ScanTableSource.ScanRuntimeProvider provider =
                ((PscDynamicSource) tableSource)
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertThat(provider).isInstanceOf(DataStreamScanProvider.class);
        final PscSource<?> pscSource = assertPscSource(provider);
        validator.accept(pscSource);
    }

    @Test
    public void testTableSink() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> {
                            options.put("sink.delivery-guarantee", "exactly-once");
                            options.put("sink.transactional-id-prefix", "psc-sink");
                        });
        final DynamicTableSink actualSink = createTableSink(SCHEMA, modifiedOptions);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                new EncodingFormatMock(",");

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueEncodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        TOPIC_URI,
                        PSC_SINK_PROPERTIES,
                        new FlinkFixedPartitioner<>(),
                        DeliveryGuarantee.EXACTLY_ONCE,
                        null,
                        "psc-sink");
        assertThat(actualSink).isEqualTo(expectedSink);

        // Test PSC producer.
        final PscDynamicSink actualPscSink = (PscDynamicSink) actualSink;
        DynamicTableSink.SinkRuntimeProvider provider =
                actualPscSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider).isInstanceOf(SinkV2Provider.class);
        final SinkV2Provider sinkProvider = (SinkV2Provider) provider;
        final Sink<RowData> sinkFunction = sinkProvider.createSink();
        assertThat(sinkFunction).isInstanceOf(PscSink.class);
    }

    @Test
    public void testTableSinkSemanticTranslation() {
        final List<String> semantics = Arrays.asList("exactly-once", "at-least-once", "none");
        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                new EncodingFormatMock(",");
        for (final String semantic : semantics) {
            final Map<String, String> modifiedOptions =
                    getModifiedOptions(
                            getBasicSinkOptions(),
                            options -> {
                                options.put("sink.semantic", semantic);
                                options.put("sink.transactional-id-prefix", "psc-sink");
                            });
            final DynamicTableSink actualSink = createTableSink(SCHEMA, modifiedOptions);
            final DynamicTableSink expectedSink =
                    createExpectedSink(
                            SCHEMA_DATA_TYPE,
                            null,
                            valueEncodingFormat,
                            new int[0],
                            new int[] {0, 1, 2},
                            null,
                            TOPIC_URI,
                            PSC_SINK_PROPERTIES,
                            new FlinkFixedPartitioner<>(),
                            DeliveryGuarantee.valueOf(semantic.toUpperCase().replace("-", "_")),
                            null,
                            "psc-sink");
            assertThat(actualSink).isEqualTo(expectedSink);
        }
    }

    @Test
    public void testTableSinkWithKeyValue() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getKeyValueSinkOptions(),
                        options -> {
                            options.put("sink.delivery-guarantee", "exactly-once");
                            options.put("sink.transactional-id-prefix", "psc-sink");
                        });
        final DynamicTableSink actualSink = createTableSink(SCHEMA, modifiedOptions);
        final PscDynamicSink actualPscSink = (PscDynamicSink) actualSink;
        // initialize stateful testing formats
        actualPscSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        final EncodingFormatMock keyEncodingFormat = new EncodingFormatMock("#");
        keyEncodingFormat.consumedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING().notNull())).notNull();

        final EncodingFormatMock valueEncodingFormat = new EncodingFormatMock("|");
        valueEncodingFormat.consumedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD(TIME, DataTypes.TIMESTAMP(3)))
                        .notNull();

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SCHEMA_DATA_TYPE,
                        keyEncodingFormat,
                        valueEncodingFormat,
                        new int[] {0},
                        new int[] {1, 2},
                        null,
                        TOPIC_URI,
                        PSC_KV_SINK_PROPERTIES,
                        new FlinkFixedPartitioner<>(),
                        DeliveryGuarantee.EXACTLY_ONCE,
                        null,
                        "psc-sink");

        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    public void testTableSinkWithParallelism() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(), options -> options.put("sink.parallelism", "100"));
        PscDynamicSink actualSink = (PscDynamicSink) createTableSink(SCHEMA, modifiedOptions);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                new EncodingFormatMock(",");

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueEncodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        TOPIC_URI,
                        PSC_SINK_PROPERTIES,
                        new FlinkFixedPartitioner<>(),
                        DeliveryGuarantee.EXACTLY_ONCE,
                        100,
                        "psc-sink");
        assertThat(actualSink).isEqualTo(expectedSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider).isInstanceOf(SinkV2Provider.class);
        final SinkV2Provider sinkProvider = (SinkV2Provider) provider;
        assertThat(sinkProvider.getParallelism().isPresent()).isTrue();
        assertThat((long) sinkProvider.getParallelism().get()).isEqualTo(100);
    }

    @Test
    public void testTableSinkAutoCompleteSchemaRegistrySubject() {
        // only format
        verifyEncoderSubject(
                options -> {
                    options.put("format", "debezium-avro-confluent");
                    options.put("debezium-avro-confluent.url", TEST_REGISTRY_URL);
                },
                PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + DEFAULT_VALUE_SUBJECT,
                "N/A");

        // only value.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                },
                PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + DEFAULT_VALUE_SUBJECT,
                "N/A");

        // value.format + key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.fields", NAME);
                },
                PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + DEFAULT_VALUE_SUBJECT,
                PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + DEFAULT_KEY_SUBJECT
                );

        // value.format + non-avro key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "csv");
                    options.put("key.fields", NAME);
                },
                PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + DEFAULT_VALUE_SUBJECT,
                "N/A");

        // non-avro value.format + key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "json");
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.fields", NAME);
                },
                "N/A",
                PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + DEFAULT_KEY_SUBJECT);

        // not override for 'format'
        verifyEncoderSubject(
                options -> {
                    options.put("format", "debezium-avro-confluent");
                    options.put("debezium-avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("debezium-avro-confluent.subject", "sub1");
                },
                "sub1",
                "N/A");

        // not override for 'key.format'
        verifyEncoderSubject(
                options -> {
                    options.put("format", "avro-confluent");
                    options.put("avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.avro-confluent.subject", "sub2");
                    options.put("key.fields", NAME);
                },
                PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + DEFAULT_VALUE_SUBJECT,
                "sub2");
    }

    private void verifyEncoderSubject(
            Consumer<Map<String, String>> optionModifier,
            String expectedValueSubject,
            String expectedKeySubject) {
        Map<String, String> options = new HashMap<>();
        // Psc specific options.
        options.put("connector", PscDynamicTableFactory.IDENTIFIER);
        options.put("topic-uri", TOPIC_URI);
        options.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        options.put("properties." + PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        options.put("properties.client.id.prefix", "test");
        options.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "AUTO_GEN_UUID");
        optionModifier.accept(options);

        final RowType rowType = (RowType) SCHEMA_DATA_TYPE.getLogicalType();
        final String valueFormat =
                options.getOrDefault(
                        FactoryUtil.FORMAT.key(),
                        options.get(PscConnectorOptions.VALUE_FORMAT.key()));
        final String keyFormat = options.get(PscConnectorOptions.KEY_FORMAT.key());

        PscDynamicSink sink = (PscDynamicSink) createTableSink(SCHEMA, options);
        final Set<String> avroFormats = new HashSet<>();
        avroFormats.add(AVRO_CONFLUENT);
        avroFormats.add(DEBEZIUM_AVRO_CONFLUENT);

        if (avroFormats.contains(valueFormat)) {
            SerializationSchema<RowData> actualValueEncoder =
                    sink.valueEncodingFormat.createRuntimeEncoder(
                            new SinkRuntimeProviderContext(false), SCHEMA_DATA_TYPE);
            final SerializationSchema<RowData> expectedValueEncoder;
            if (AVRO_CONFLUENT.equals(valueFormat)) {
                expectedValueEncoder = createConfluentAvroSerSchema(rowType, expectedValueSubject);
            } else {
                expectedValueEncoder = createDebeziumAvroSerSchema(rowType, expectedValueSubject);
            }
            assertThat(actualValueEncoder).isEqualTo(expectedValueEncoder);
        }

        if (avroFormats.contains(keyFormat)) {
            assertThat(sink.keyEncodingFormat).isNotNull();
            SerializationSchema<RowData> actualKeyEncoder =
                    sink.keyEncodingFormat.createRuntimeEncoder(
                            new SinkRuntimeProviderContext(false), SCHEMA_DATA_TYPE);
            final SerializationSchema<RowData> expectedKeyEncoder;
            if (AVRO_CONFLUENT.equals(keyFormat)) {
                expectedKeyEncoder = createConfluentAvroSerSchema(rowType, expectedKeySubject);
            } else {
                expectedKeyEncoder = createDebeziumAvroSerSchema(rowType, expectedKeySubject);
            }
            assertThat(actualKeyEncoder).isEqualTo(expectedKeyEncoder);
        }
    }

    private SerializationSchema<RowData> createConfluentAvroSerSchema(
            RowType rowType, String subject) {
        return new AvroRowDataSerializationSchema(
                rowType,
                ConfluentRegistryAvroSerializationSchema.forGeneric(
                        subject, AvroSchemaConverter.convertToSchema(rowType), TEST_REGISTRY_URL),
                RowDataToAvroConverters.createConverter(rowType));
    }

    private SerializationSchema<RowData> createDebeziumAvroSerSchema(
            RowType rowType, String subject) {
        return new DebeziumAvroSerializationSchema(rowType, TEST_REGISTRY_URL, subject, null);
    }

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testSourceTableWithTopicAndTopicPattern() {
        assertThatThrownBy(
                        () -> {
                            final Map<String, String> modifiedOptions =
                                    getModifiedOptions(
                                            getBasicSourceOptions(),
                                            options -> {
                                                options.put("topic-uri", TOPIC_URIS);
                                                options.put("topic-pattern", TOPIC_REGEX);
                                            });

                            createTableSource(SCHEMA, modifiedOptions);
                        })
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Option 'topic-uri' and 'topic-pattern' shouldn't be set together."));
    }

    @Test
    public void testMissingStartupTimestamp() {
        assertThatThrownBy(
                        () -> {
                            final Map<String, String> modifiedOptions =
                                    getModifiedOptions(
                                            getBasicSourceOptions(),
                                            options ->
                                                    options.put("scan.startup.mode", "timestamp"));

                            createTableSource(SCHEMA, modifiedOptions);
                        })
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "'scan.startup.timestamp-millis' "
                                        + "is required in 'timestamp' startup mode but missing."));
    }

    @Test
    public void testMissingSpecificOffsets() {
        assertThatThrownBy(
                        () -> {
                            final Map<String, String> modifiedOptions =
                                    getModifiedOptions(
                                            getBasicSourceOptions(),
                                            options ->
                                                    options.remove(
                                                            "scan.startup.specific-offsets"));

                            createTableSource(SCHEMA, modifiedOptions);
                        })
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "'scan.startup.specific-offsets' "
                                        + "is required in 'specific-offsets' startup mode but missing."));
    }

    @Test
    public void testInvalidSinkPartitioner() {
        assertThatThrownBy(
                        () -> {
                            final Map<String, String> modifiedOptions =
                                    getModifiedOptions(
                                            getBasicSinkOptions(),
                                            options -> options.put("sink.partitioner", "abc"));

                            createTableSink(SCHEMA, modifiedOptions);
                        })
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Could not find and instantiate partitioner " + "class 'abc'"));
    }

    @Test
    public void testInvalidRoundRobinPartitionerWithKeyFields() {
        assertThatThrownBy(
                        () -> {
                            final Map<String, String> modifiedOptions =
                                    getModifiedOptions(
                                            getKeyValueSinkOptions(),
                                            options ->
                                                    options.put("sink.partitioner", "round-robin"));

                            createTableSink(SCHEMA, modifiedOptions);
                        })
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Currently 'round-robin' partitioner only works "
                                        + "when option 'key.fields' is not specified."));
    }

    @Test
    public void testExactlyOnceGuaranteeWithoutTransactionalIdPrefix() {
        assertThatThrownBy(
                        () -> {
                            final Map<String, String> modifiedOptions =
                                    getModifiedOptions(
                                            getKeyValueSinkOptions(),
                                            options -> {
                                                options.remove(
                                                        PscConnectorOptions
                                                                .TRANSACTIONAL_ID_PREFIX
                                                                .key());
                                                options.put(
                                                        PscConnectorOptions.DELIVERY_GUARANTEE
                                                                .key(),
                                                        DeliveryGuarantee.EXACTLY_ONCE.toString());
                                            });
                            createTableSink(SCHEMA, modifiedOptions);
                        })
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "sink.transactional-id-prefix must be specified when using DeliveryGuarantee.EXACTLY_ONCE."));
    }

    @Test
    public void testSinkWithTopicListOrTopicPattern() {
        Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> {
                            options.put("topic-uri", TOPIC_URIS);
                            options.put("scan.startup.mode", "earliest-offset");
                            options.remove("specific-offsets");
                        });
        final String errorMessageTemp =
                "Flink PSC sink currently only supports single topic, but got %s: %s.";

        try {
            createTableSink(SCHEMA, modifiedOptions);
        } catch (Throwable t) {
            assertThat(t.getCause().getMessage())
                    .isEqualTo(
                            String.format(
                                    errorMessageTemp,
                                    "'topic-uri'",
                                    String.format("[%s]", String.join(", ", TOPIC_URI_LIST))));
        }

        modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> options.put("topic-pattern", TOPIC_REGEX));

        try {
            createTableSink(SCHEMA, modifiedOptions);
        } catch (Throwable t) {
            assertThat(t.getCause().getMessage())
                    .isEqualTo(String.format(errorMessageTemp, "'topic-pattern'", TOPIC_REGEX));
        }
    }

    @Test
    public void testPrimaryKeyValidation() {
        final ResolvedSchema pkSchema =
                new ResolvedSchema(
                        SCHEMA.getColumns(),
                        SCHEMA.getWatermarkSpecs(),
                        UniqueConstraint.primaryKey(NAME, Collections.singletonList(NAME)));

        Map<String, String> sinkOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options ->
                                options.put(
                                        String.format(
                                                "%s.%s",
                                                TestFormatFactory.IDENTIFIER,
                                                TestFormatFactory.CHANGELOG_MODE.key()),
                                        "I;UA;UB;D"));
        // pk can be defined on cdc table, should pass
        createTableSink(pkSchema, sinkOptions);

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(pkSchema, getBasicSinkOptions()))
                .havingRootCause()
                .withMessage(
                        "The PSC table 'default.default.t1' with 'test-format' format"
                                + " doesn't support defining PRIMARY KEY constraint on the table, because it can't"
                                + " guarantee the semantic of primary key.");

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(pkSchema, getKeyValueSinkOptions()))
                .havingRootCause()
                .withMessage(
                        "The PSC table 'default.default.t1' with 'test-format' format"
                                + " doesn't support defining PRIMARY KEY constraint on the table, because it can't"
                                + " guarantee the semantic of primary key.");

        Map<String, String> sourceOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options ->
                                options.put(
                                        String.format(
                                                "%s.%s",
                                                TestFormatFactory.IDENTIFIER,
                                                TestFormatFactory.CHANGELOG_MODE.key()),
                                        "I;UA;UB;D"));
        // pk can be defined on cdc table, should pass
        createTableSource(pkSchema, sourceOptions);

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSource(pkSchema, getBasicSourceOptions()))
                .havingRootCause()
                .withMessage(
                        "The PSC table 'default.default.t1' with 'test-format' format"
                                + " doesn't support defining PRIMARY KEY constraint on the table, because it can't"
                                + " guarantee the semantic of primary key.");
    }

    @Test
    public void testDiscoverPartitionByDefault() {
        Map<String, String> tableSourceOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> options.remove("scan.topic-partition-discovery.interval"));
        final PscDynamicSource actualSource =
                (PscDynamicSource) createTableSource(SCHEMA, tableSourceOptions);
        Properties props = new Properties();
        props.putAll(PSC_SOURCE_PROPERTIES);
        // The default partition discovery interval is 5 minutes
        props.setProperty("partition.discovery.interval.ms", "300000");
        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_0), OFFSET_0);
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_1), OFFSET_1);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);
        // Test scan source equals
        final PscDynamicSource expectedKafkaSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        Collections.singletonList(TOPIC_URI),
                        null,
                        props,
                        StartupMode.SPECIFIC_OFFSETS,
                        specificOffsets,
                        0);
        assertThat(actualSource).isEqualTo(expectedKafkaSource);
        ScanTableSource.ScanRuntimeProvider provider =
                actualSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertPscSource(provider);
    }

    @Test
    public void testDisableDiscoverPartition() {
        Map<String, String> tableSourceOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> options.put("scan.topic-partition-discovery.interval", "0"));
        final PscDynamicSource actualSource =
                (PscDynamicSource) createTableSource(SCHEMA, tableSourceOptions);
        Properties props = new Properties();
        props.putAll(PSC_SOURCE_PROPERTIES);
        // Disable discovery if the partition discovery interval is 0 minutes
        props.setProperty("partition.discovery.interval.ms", "0");
        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_0), OFFSET_0);
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_1), OFFSET_1);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);
        // Test scan source equals
        final PscDynamicSource expectedKafkaSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        Collections.singletonList(TOPIC_URI),
                        null,
                        props,
                        StartupMode.SPECIFIC_OFFSETS,
                        specificOffsets,
                        0);
        assertThat(actualSource).isEqualTo(expectedKafkaSource);
        ScanTableSource.ScanRuntimeProvider provider =
                actualSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertPscSource(provider);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static PscDynamicSource createExpectedScanSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> topics,
            @Nullable Pattern topicPattern,
            Properties properties,
            StartupMode startupMode,
            Map<PscTopicUriPartition, Long> specificStartupOffsets,
            long startupTimestampMillis) {
        return new PscDynamicSource(
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                BoundedMode.UNBOUNDED,
                Collections.emptyMap(),
                0,
                false,
                FactoryMocks.IDENTIFIER.asSummaryString());
    }

    private static PscDynamicSink createExpectedSink(
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String topic,
            Properties properties,
            @Nullable FlinkPscPartitioner<RowData> partitioner,
            DeliveryGuarantee deliveryGuarantee,
            @Nullable Integer parallelism,
            String transactionalIdPrefix) {
        return new PscDynamicSink(
                physicalDataType,
                physicalDataType,
                keyEncodingFormat,
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topic,
                properties,
                partitioner,
                deliveryGuarantee,
                false,
                SinkBufferFlushMode.DISABLED,
                parallelism,
                transactionalIdPrefix);
    }

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private static Map<String, String> getModifiedOptions(
            Map<String, String> options, Consumer<Map<String, String>> optionModifier) {
        optionModifier.accept(options);
        return options;
    }

    private static Map<String, String> getBasicSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // PSC specific options.
        tableOptions.put("connector", PscDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic-uri", TOPIC_URI);
        tableOptions.put("properties.psc.consumer.group.id", "dummy");
        tableOptions.put("properties.psc.cluster.uri", PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        tableOptions.put("properties.client.id.prefix", "test");
        tableOptions.put("scan.startup.mode", "specific-offsets");
        tableOptions.put("scan.startup.specific-offsets", PROPS_SCAN_OFFSETS);
        tableOptions.put("scan.topic-partition-discovery.interval", DISCOVERY_INTERVAL);
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey =
                String.format(
                        "%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        final String failOnMissingKey =
                String.format(
                        "%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());
        tableOptions.put(formatDelimiterKey, ",");
        tableOptions.put(failOnMissingKey, "true");
        return tableOptions;
    }

    private static Map<String, String> getBasicSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // PSC specific options.
        tableOptions.put("connector", PscDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic-uri", TOPIC_URI);
        tableOptions.put("properties." + PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "test-producer");
        tableOptions.put(
                "sink.partitioner", PscConnectorOptionsUtil.SINK_PARTITIONER_VALUE_FIXED);
        tableOptions.put("sink.delivery-guarantee", DeliveryGuarantee.EXACTLY_ONCE.toString());
        tableOptions.put("sink.transactional-id-prefix", "psc-sink");
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey =
                String.format(
                        "%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        tableOptions.put(formatDelimiterKey, ",");
        return tableOptions;
    }

    private static Map<String, String> getKeyValueSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // PSC specific options.
        tableOptions.put("connector", PscDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic-uri", TOPIC_URI);
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        tableOptions.put("properties." + PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        tableOptions.put("properties.client.id.prefix", "test");
        tableOptions.put("scan.topic-partition-discovery.interval", DISCOVERY_INTERVAL);
        // Format options.
        tableOptions.put("key.format", TestFormatFactory.IDENTIFIER);
        tableOptions.put(
                String.format(
                        "key.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                "#");
        tableOptions.put("key.fields", NAME);
        tableOptions.put("value.format", TestFormatFactory.IDENTIFIER);
        tableOptions.put(
                String.format(
                        "value.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                "|");
        tableOptions.put(
                "value.fields-include",
                PscConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY.toString());
        return tableOptions;
    }

    private static Map<String, String> getKeyValueSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // PSC specific options.
        tableOptions.put("connector", PscDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic-uri", TOPIC_URI);
        tableOptions.put("properties." + PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        tableOptions.put("properties.client.id.prefix", "test");
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "test-producer");
        tableOptions.put(
                "sink.partitioner", PscConnectorOptionsUtil.SINK_PARTITIONER_VALUE_FIXED);
        tableOptions.put("sink.delivery-guarantee", DeliveryGuarantee.EXACTLY_ONCE.toString());
        tableOptions.put("sink.transactional-id-prefix", "psc-sink");
        // Format options.
        tableOptions.put("key.format", TestFormatFactory.IDENTIFIER);
        tableOptions.put(
                String.format(
                        "key.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                "#");
        tableOptions.put("key.fields", NAME);
        tableOptions.put("value.format", TestFormatFactory.IDENTIFIER);
        tableOptions.put(
                String.format(
                        "value.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                "|");
        tableOptions.put(
                "value.fields-include",
                PscConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY.toString());
        return tableOptions;
    }

    private PscSource<?> assertPscSource(ScanTableSource.ScanRuntimeProvider provider) {
        assertThat(provider).isInstanceOf(DataStreamScanProvider.class);
        final DataStreamScanProvider dataStreamScanProvider = (DataStreamScanProvider) provider;
        final Transformation<RowData> transformation =
                dataStreamScanProvider
                        .produceDataStream(
                                n -> Optional.empty(),
                                StreamExecutionEnvironment.createLocalEnvironment())
                        .getTransformation();
        assertThat(transformation).isInstanceOf(SourceTransformation.class);
        SourceTransformation<RowData, PscTopicUriPartitionSplit, PscSourceEnumState>
                sourceTransformation =
                        (SourceTransformation<RowData, PscTopicUriPartitionSplit, PscSourceEnumState>)
                                transformation;
        assertThat(sourceTransformation.getSource()).isInstanceOf(PscSource.class);
        return (PscSource<?>) sourceTransformation.getSource();
    }
}
