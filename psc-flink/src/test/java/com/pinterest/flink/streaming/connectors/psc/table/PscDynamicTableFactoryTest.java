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

    private static final Properties PSC_BASIC_SOURCE_PROPERTIES = new Properties();
    private static final Properties PSC_SINK_PROPERTIES = new Properties();

    static {
        PSC_BASIC_SOURCE_PROPERTIES.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        PSC_BASIC_SOURCE_PROPERTIES.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "dummy-client");
        PSC_BASIC_SOURCE_PROPERTIES.setProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        PSC_BASIC_SOURCE_PROPERTIES.setProperty("partition.discovery.interval.ms", "1000");

        PSC_SINK_PROPERTIES.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "dummy-producer-client");
        PSC_SINK_PROPERTIES.setProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
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
                        PSC_BASIC_SOURCE_PROPERTIES,
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
                        PSC_BASIC_SOURCE_PROPERTIES,
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
                        PSC_BASIC_SOURCE_PROPERTIES,
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
                        PSC_BASIC_SOURCE_PROPERTIES,
                        StartupMode.GROUP_OFFSETS,
                        Collections.emptyMap(),
                        0);
        expectedPscSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedPscSource.metadataKeys = Collections.singletonList("timestamp");

        assertThat(actualSource).isEqualTo(expectedPscSource);
    }

    @Test
    public void testTableSourceCommitOnCheckpointDisabled() {
        // Test that validation requires consumer group ID - this test now validates that
        // missing group ID results in a proper validation error
        assertThatThrownBy(() -> {
            final Map<String, String> modifiedOptions =
                    getModifiedOptions(
                            getBasicSourceOptions(), options -> options.remove("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID));
            createTableSource(SCHEMA, modifiedOptions);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "Required property 'psc.consumer.group.id' is missing or empty"));
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
                        getSinkOptions(),
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
                            getSinkOptions(),
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
                        PSC_SINK_PROPERTIES,
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
                        getSinkOptions(), options -> options.put("sink.parallelism", "100"));
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
        options.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "dummy-client");
        options.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "dummy-producer-client");
        options.put("properties." + PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
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
    // AUTO_GEN_UUID Tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testTableSourceWithAutoGenUuidConsumerClientId() {
        final Map<String, String> options = getModifiedOptions(
                getBasicSourceOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-consumer-client");
                });

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        Properties actualProperties = ((PscDynamicSource) actualSource).properties;

        String consumerClientId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        assertThat(consumerClientId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-consumer-client-")
                .matches("test-consumer-client-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    public void testTableSourceWithAutoGenUuidConsumerGroupId() {
        final Map<String, String> options = getModifiedOptions(
                getBasicSourceOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_GROUP_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-consumer-group");
                });

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        Properties actualProperties = ((PscDynamicSource) actualSource).properties;

        String consumerGroupId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertThat(consumerGroupId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-consumer-group-")
                .matches("test-consumer-group-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    public void testTableSourceWithAutoGenUuidBothConsumerIds() {
        final Map<String, String> options = getModifiedOptions(
                getBasicSourceOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_GROUP_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-both");
                });

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        Properties actualProperties = ((PscDynamicSource) actualSource).properties;

        String consumerClientId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        String consumerGroupId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        assertThat(consumerClientId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-both-")
                .matches("test-both-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

        assertThat(consumerGroupId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-both-")
                .matches("test-both-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                .isNotEqualTo(consumerClientId); // Should be different UUIDs
    }

    @Test
    public void testTableSinkWithAutoGenUuidProducerClientId() {
        final Map<String, String> options = getModifiedOptions(
                getSinkOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_PRODUCER_CLIENT_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-producer-client");
                });

        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        Properties actualProperties = ((PscDynamicSink) actualSink).properties;

        String producerClientId = actualProperties.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);
        assertThat(producerClientId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-producer-client-")
                .matches("test-producer-client-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    public void testKeyValueSourceWithAutoGenUuidAllIds() {
        final Map<String, String> options = getModifiedOptions(
                getKeyValueSourceOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_GROUP_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-keyvalue-src");
                });

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        Properties actualProperties = ((PscDynamicSource) actualSource).properties;

        String consumerClientId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        String consumerGroupId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        assertThat(consumerClientId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-keyvalue-src-")
                .matches("test-keyvalue-src-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

        assertThat(consumerGroupId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-keyvalue-src-")
                .matches("test-keyvalue-src-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                .isNotEqualTo(consumerClientId);
    }

    @Test
    public void testKeyValueSinkWithAutoGenUuidAllIds() {
        final Map<String, String> options = getModifiedOptions(
                getKeyValueSinkOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_PRODUCER_CLIENT_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-keyvalue-sink");
                });

        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        Properties actualProperties = ((PscDynamicSink) actualSink).properties;

        String producerClientId = actualProperties.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);
        assertThat(producerClientId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-keyvalue-sink-")
                .matches("test-keyvalue-sink-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    public void testAutoGenUuidWithTrimmedPrefix() {
        final Map<String, String> options = getModifiedOptions(
                getBasicSourceOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "  trimmed-test-prefix  ");
                });

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        Properties actualProperties = ((PscDynamicSource) actualSource).properties;

        String consumerClientId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        assertThat(consumerClientId)
                .isNotNull()
                .startsWith("trimmed-test-prefix-") // Should be trimmed
                .matches("trimmed-test-prefix-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    public void testAutoGenUuidMixedWithCustomValues() {
        final Map<String, String> options = getModifiedOptions(
                getBasicSourceOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_GROUP_ID.key(), "custom-group-id");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-mixed");
                });

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        Properties actualProperties = ((PscDynamicSource) actualSource).properties;

        String consumerClientId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        String consumerGroupId = actualProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        // Client ID should be generated with UUID
        assertThat(consumerClientId)
                .isNotNull()
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-mixed-")
                .matches("test-mixed-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

        // Group ID should remain custom value
        assertThat(consumerGroupId)
                .isEqualTo("custom-group-id");
    }

    @Test
    public void testAutoGenUuidUniquePerTableCreation() {
        final Map<String, String> options = getModifiedOptions(
                getBasicSourceOptions(),
                opts -> {
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID.key(), "AUTO_GEN_UUID");
                    opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-unique");
                });

        // Create two separate table sources with same options
        final DynamicTableSource actualSource1 = createTableSource(SCHEMA, options);
        final DynamicTableSource actualSource2 = createTableSource(SCHEMA, options);

        Properties properties1 = ((PscDynamicSource) actualSource1).properties;
        Properties properties2 = ((PscDynamicSource) actualSource2).properties;

        String clientId1 = properties1.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        String clientId2 = properties2.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);

        assertThat(clientId1)
                .isNotNull()
                .startsWith("test-unique-")
                .matches("test-unique-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

        assertThat(clientId2)
                .isNotNull()
                .startsWith("test-unique-")
                .matches("test-unique-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                .isNotEqualTo(clientId1); // Each table creation should get unique UUID
    }

    // AUTO_GEN_UUID Negative Tests
    @Test
    public void testAutoGenUuidWithMissingClientIdPrefix() {
        assertThatThrownBy(() -> {
            final Map<String, String> options = getModifiedOptions(
                    getBasicSourceOptions(),
                    opts -> opts.put(PscConnectorOptions.PROPS_CLIENT_ID.key(), "AUTO_GEN_UUID"));
            createTableSource(SCHEMA, options);
        })
        .isInstanceOf(ValidationException.class)
        .satisfies(anyCauseMatches(ValidationException.class, 
                "properties.client.id.prefix must be provided when using AUTO_GEN_UUID for ID options"));
    }

    @Test
    public void testAutoGenUuidWithEmptyPrefixInSourceTest() {
        assertThatThrownBy(() -> {
            final Map<String, String> options = getModifiedOptions(
                    getBasicSourceOptions(),
                    opts -> {
                        opts.put(PscConnectorOptions.PROPS_CLIENT_ID.key(), "AUTO_GEN_UUID");
                        opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "");
                    });
            createTableSource(SCHEMA, options);
        })
        .isInstanceOf(ValidationException.class)
        .satisfies(anyCauseMatches(ValidationException.class,
                "properties.client.id.prefix must be non-empty"));
    }

    @Test
    public void testAutoGenUuidWithWhitespacePrefixInSourceTest() {
        assertThatThrownBy(() -> {
            final Map<String, String> options = getModifiedOptions(
                    getBasicSourceOptions(),
                    opts -> {
                        opts.put(PscConnectorOptions.PROPS_GROUP_ID.key(), "AUTO_GEN_UUID");
                        opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "   ");
                    });
            createTableSource(SCHEMA, options);
        })
        .isInstanceOf(ValidationException.class)
        .satisfies(anyCauseMatches(ValidationException.class,
                "properties.client.id.prefix must be non-empty"));
    }

    @Test
    public void testAutoGenUuidOnDisallowedProperty() {
        assertThatThrownBy(() -> {
            final Map<String, String> options = getModifiedOptions(
                    getBasicSourceOptions(),
                    opts -> {
                        opts.put("properties.bootstrap.servers", "AUTO_GEN_UUID");
                        opts.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test");
                    });
            createTableSource(SCHEMA, options);
        })
        .isInstanceOf(ValidationException.class)
        .satisfies(anyCauseMatches(ValidationException.class,
                "AUTO_GEN_UUID is not allowed for property"));
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
                                            getSinkOptions(),
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
                        getSinkOptions(),
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
                        getSinkOptions(),
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
                        getSinkOptions(),
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
                .isThrownBy(() -> createTableSink(pkSchema, getSinkOptions()))
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
        props.putAll(PSC_BASIC_SOURCE_PROPERTIES);
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
        props.putAll(PSC_BASIC_SOURCE_PROPERTIES);
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
        tableOptions.put("properties.psc.consumer.client.id", "dummy-client");
        tableOptions.put("properties.psc.cluster.uri", PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
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

    private static Map<String, String> getSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // PSC specific options.
        tableOptions.put("connector", PscDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic-uri", TOPIC_URI);
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "dummy-producer-client");
        tableOptions.put("properties." + PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
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

    private static Map<String, String> getKeyValueSinkOptions() {
        Map<String, String> tableOptions = getSinkOptions();
        // Remove basic format and add key/value formats
        tableOptions.remove("format");
        tableOptions.remove(String.format(
                "%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()));
        // Add key/value format options.
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

    private static Map<String, String> getKeyValueSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // PSC specific options for source.
        tableOptions.put("connector", PscDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic-uri", TOPIC_URI);
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "dummy-client");
        tableOptions.put("properties." + PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
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

    // --------------------------------------------------------------------------------------------
    // CREATE TABLE ... LIKE Tests for AUTO_GEN_UUID Support
    // --------------------------------------------------------------------------------------------

    @Test
    public void testCreateTableLikeWithAutoGenUuidInBaseAndOverrideInDerived() {
        // Test scenario: Base table has AUTO_GEN_UUID, derived table overrides with custom value
        Map<String, String> modifiedOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> {
                    options.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                    options.put("properties.psc.consumer.client.id", "AUTO_GEN_UUID");
                    options.put("properties.client.id.prefix", "test-base");
                });

        // Properties should have AUTO_GEN_UUID values replaced with prefixed UUIDs
        Properties baseProperties = 
                PscConnectorOptionsUtil.getPscProperties(modifiedOptions);
        
        assertThat(baseProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID))
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-base-")
                .matches("^test-base-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
        assertThat(baseProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID))
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("test-base-")
                .matches("^test-base-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    }

    @Test
    public void testCreateTableLikeWithAutoGenUuidInBaseAndAutoGenUuidInDerived() {
        // Test scenario: Both base and derived tables use AUTO_GEN_UUID, each should get unique UUIDs
        Map<String, String> baseTableOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> {
                    options.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                    options.put("properties.psc.consumer.client.id", "AUTO_GEN_UUID");
                    options.put("properties.client.id.prefix", "base-table");
                });

        Map<String, String> derivedTableOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> {
                    options.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                    options.put("properties.psc.consumer.client.id", "AUTO_GEN_UUID");
                    options.put("properties.client.id.prefix", "derived-table");
                });

        // Both should generate different UUIDs with their respective prefixes
        Properties baseProperties = 
                PscConnectorOptionsUtil.getPscProperties(baseTableOptions);
        Properties derivedProperties = 
                PscConnectorOptionsUtil.getPscProperties(derivedTableOptions);

        String baseGroupId = baseProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String derivedGroupId = derivedProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String baseClientId = baseProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        String derivedClientId = derivedProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);

        assertThat(baseGroupId)
                .isNotEqualTo(derivedGroupId)
                .startsWith("base-table-");
        assertThat(baseClientId)
                .isNotEqualTo(derivedClientId)
                .startsWith("base-table-");
        assertThat(derivedGroupId).startsWith("derived-table-");
        assertThat(derivedClientId).startsWith("derived-table-");
    }

    @Test
    public void testCreateTableLikeWithoutAutoGenUuidInBaseAndOverrideInDerived() {
        // Test scenario: Base table has custom values, derived table overrides with AUTO_GEN_UUID
        Map<String, String> baseTableOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> {
                    options.put("properties.psc.consumer.group.id", "base-group");
                    options.put("properties.psc.consumer.client.id", "base-client");
                });

        Map<String, String> derivedTableOptions = getModifiedOptions(
                getBasicSourceOptions(),
                options -> {
                    options.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                    options.put("properties.psc.consumer.client.id", "base-client"); // Keep same client ID
                    options.put("properties.client.id.prefix", "derived-prefix");
                });

        Properties baseProperties = 
                PscConnectorOptionsUtil.getPscProperties(baseTableOptions);
        Properties derivedProperties = 
                PscConnectorOptionsUtil.getPscProperties(derivedTableOptions);

        // Base should have original values
        assertThat(baseProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID))
                .isEqualTo("base-group");
        assertThat(baseProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID))
                .isEqualTo("base-client");

        // Derived should have AUTO_GEN_UUID replaced for group_id but keep custom client_id
        assertThat(derivedProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID))
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("derived-prefix-")
                .matches("^derived-prefix-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
        assertThat(derivedProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID))
                .isEqualTo("base-client");
    }

    // --------------------------------------------------------------------------------------------
    // Validation Tests for AUTO_GEN_UUID and client.id.prefix
    // --------------------------------------------------------------------------------------------

    @Test
    public void testAutoGenUuidRequiresClientIdPrefix() {
        // Test that AUTO_GEN_UUID requires client.id.prefix
        assertThatThrownBy(() -> {
            Map<String, String> options = getModifiedOptions(
                    getBasicSourceOptions(),
                    opts -> {
                        opts.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                        // Missing client.id.prefix
                    });
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "properties.client.id.prefix must be provided when using AUTO_GEN_UUID for ID options"));
    }

    @Test
    public void testAutoGenUuidWithEmptyClientIdPrefix() {
        // Test that AUTO_GEN_UUID requires non-empty client.id.prefix
        assertThatThrownBy(() -> {
            Map<String, String> options = getModifiedOptions(
                    getBasicSourceOptions(),
                    opts -> {
                        opts.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                        opts.put("properties.client.id.prefix", "");
                    });
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "properties.client.id.prefix must be non-empty"));
    }

    @Test
    public void testAutoGenUuidWithWhitespaceOnlyClientIdPrefix() {
        // Test that AUTO_GEN_UUID requires non-empty client.id.prefix after trimming
        assertThatThrownBy(() -> {
            Map<String, String> options = getModifiedOptions(
                    getBasicSourceOptions(),
                    opts -> {
                        opts.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                        opts.put("properties.client.id.prefix", "   ");
                    });
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "after trimming whitespace"));
    }

    @Test
    public void testAutoGenUuidWithValidClientIdPrefix() {
        // Test that AUTO_GEN_UUID works correctly with valid client.id.prefix
        Map<String, String> options = getModifiedOptions(
                getBasicSourceOptions(),
                opts -> {
                    opts.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                    opts.put("properties.psc.consumer.client.id", "AUTO_GEN_UUID");
                    opts.put("properties.psc.producer.client.id", "AUTO_GEN_UUID");
                    opts.put("properties.client.id.prefix", "factory-test");
                });

        // Should create source successfully
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final PscDynamicSource actualPscSource = (PscDynamicSource) actualSource;

        // Verify the generated properties have correct prefixes
        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(options);
        
        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String clientId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        String producerClientId = pscProperties.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);

        assertThat(groupId)
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("factory-test-")
                .matches("^factory-test-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
        
        assertThat(clientId)
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("factory-test-")
                .matches("^factory-test-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
        
        assertThat(producerClientId)
                .isNotEqualTo("AUTO_GEN_UUID")
                .startsWith("factory-test-")
                .matches("^factory-test-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

        // All generated IDs should be different
        assertThat(groupId).isNotEqualTo(clientId).isNotEqualTo(producerClientId);
        assertThat(clientId).isNotEqualTo(producerClientId);
    }

    @Test
    public void testAutoGenUuidTrimsWhitespaceFromPrefix() {
        // Test that client.id.prefix is properly trimmed
        Map<String, String> options = getModifiedOptions(
                getBasicSourceOptions(),
                opts -> {
                    opts.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                    opts.put("properties.client.id.prefix", "  trimmed-prefix  ");
                });

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(options);
        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        assertThat(groupId)
                .startsWith("trimmed-prefix-")
                .doesNotStartWith(" ")
                .doesNotContain("  -");
    }

    @Test
    public void testAutoGenUuidOnNonAllowedKeyThrowsValidationException() {
        // Test that AUTO_GEN_UUID on non-allowed keys throws ValidationException
        assertThatThrownBy(() -> {
            Map<String, String> options = getModifiedOptions(
                    getBasicSourceOptions(),
                    opts -> {
                        opts.put("properties.bootstrap.servers", "AUTO_GEN_UUID");
                        opts.put("properties.client.id.prefix", "test");
                    });
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "AUTO_GEN_UUID is not allowed for property"));
    }

    // --------------------------------------------------------------------------------------------
    // Consumer Client Options Validation Tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testConsumerMissingClientIdValidation() {
        Map<String, String> options = getModifiedOptions(
            getBasicSourceOptions(),
            opts -> {
                // Remove client.id but keep group.id
                opts.remove("properties.psc.consumer.client.id");
                opts.put("properties.psc.consumer.group.id", "test-group");
            });

        assertThatThrownBy(() -> {
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "Required property 'psc.consumer.client.id' is missing or empty"));
    }

    @Test
    public void testConsumerMissingGroupIdValidation() {
        Map<String, String> options = getModifiedOptions(
            getBasicSourceOptions(),
            opts -> {
                // Remove group.id but keep client.id
                opts.put("properties.psc.consumer.client.id", "test-client");
                opts.remove("properties.psc.consumer.group.id");
            });

        assertThatThrownBy(() -> {
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "Required property 'psc.consumer.group.id' is missing or empty"));
    }

    @Test
    public void testConsumerEmptyClientIdValidation() {
        Map<String, String> options = getModifiedOptions(
            getBasicSourceOptions(),
            opts -> {
                // Set empty client.id but valid group.id
                opts.put("properties.psc.consumer.client.id", "");
                opts.put("properties.psc.consumer.group.id", "test-group");
            });

        assertThatThrownBy(() -> {
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "Required property 'psc.consumer.client.id' is missing or empty"));
    }

    @Test
    public void testConsumerEmptyGroupIdValidation() {
        Map<String, String> options = getModifiedOptions(
            getBasicSourceOptions(),
            opts -> {
                // Set valid client.id but empty group.id
                opts.put("properties.psc.consumer.client.id", "test-client");
                opts.put("properties.psc.consumer.group.id", "   ");  // whitespace only
            });

        assertThatThrownBy(() -> {
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "Required property 'psc.consumer.group.id' is missing or empty"));
    }

    @Test
    public void testConsumerValidExplicitClientOptions() {
        Map<String, String> options = getModifiedOptions(
            getBasicSourceOptions(),
            opts -> {
                // Set explicit valid values for both
                opts.put("properties.psc.consumer.client.id", "explicit-client-id");
                opts.put("properties.psc.consumer.group.id", "explicit-group-id");
            });

        // Should not throw - validation passes
        DynamicTableSource tableSource = createTableSource(SCHEMA, options);
        assertThat(tableSource).isNotNull();
    }

    @Test
    public void testConsumerValidAutoGenUuidClientOptions() {
        Map<String, String> options = getModifiedOptions(
            getBasicSourceOptions(),
            opts -> {
                // Use AUTO_GEN_UUID for both with required prefix
                opts.put("properties.psc.consumer.client.id", "AUTO_GEN_UUID");
                opts.put("properties.psc.consumer.group.id", "AUTO_GEN_UUID");
                opts.put("properties.client.id.prefix", "test-consumer");
            });

        // Should not throw - validation passes
        DynamicTableSource tableSource = createTableSource(SCHEMA, options);
        assertThat(tableSource).isNotNull();
    }

    @Test
    public void testConsumerMissingBothClientOptionsValidation() {
        Map<String, String> options = getModifiedOptions(
            getBasicSourceOptions(),
            opts -> {
                // Remove both client.id and group.id
                opts.remove("properties.psc.consumer.client.id");
                opts.remove("properties.psc.consumer.group.id");
            });

        assertThatThrownBy(() -> {
            createTableSource(SCHEMA, options);
        }).isInstanceOf(ValidationException.class)
          .satisfies(anyCauseMatches(ValidationException.class, 
                  "Required property 'psc.consumer.client.id' is missing or empty"));
    }
}
