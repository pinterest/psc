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
import com.pinterest.flink.table.descriptors.psc.Psc;
import com.pinterest.flink.table.descriptors.psc.PscValidator;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.descriptors.TestTableDescriptor;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.utils.TestDeserializationSchema;
import org.apache.flink.table.factories.utils.TestSerializationSchema;
import org.apache.flink.table.factories.utils.TestTableFormat;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test base for {@link PscTableSourceSinkFactoryBase}.
 */
public abstract class PscTableSourceSinkFactoryTestBase extends TestLogger {

    private static final String TOPIC_URI =
            TopicUri.DEFAULT_PROTOCOL + ":" + TopicUri.SEPARATOR + "rn:kafka:env:cloud_region::cluster:myTopic";
    private static final int PARTITION_0 = 0;
    private static final long OFFSET_0 = 100L;
    private static final int PARTITION_1 = 1;
    private static final long OFFSET_1 = 123L;
    private static final String FRUIT_NAME = "fruit-name";
    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String EVENT_TIME = "event-time";
    private static final String PROC_TIME = "proc-time";
    private static final String WATERMARK_EXPRESSION = EVENT_TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);

    private static final Properties PSC_CONFIGURATION = new Properties();

    static {
        PSC_CONFIGURATION.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test");
    }

    private static final Map<Integer, Long> OFFSETS = new HashMap<>();

    static {
        OFFSETS.put(PARTITION_0, OFFSET_0);
        OFFSETS.put(PARTITION_1, OFFSET_1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableSource() {
        // prepare parameters for Kafka table source
        final TableSchema schema = TableSchema.builder()
                .field(FRUIT_NAME, DataTypes.STRING())
                .field(COUNT, DataTypes.DECIMAL(38, 18))
                .field(EVENT_TIME, DataTypes.TIMESTAMP(3))
                .field(PROC_TIME, DataTypes.TIMESTAMP(3))
                .build();

        final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = Collections.singletonList(
                new RowtimeAttributeDescriptor(EVENT_TIME, new ExistingField(TIME), new AscendingTimestamps()));

        final Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put(FRUIT_NAME, NAME);
        fieldMapping.put(NAME, NAME);
        fieldMapping.put(COUNT, COUNT);
        fieldMapping.put(TIME, TIME);

        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_0), OFFSET_0);
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_1), OFFSET_1);

        final TestDeserializationSchema deserializationSchema = new TestDeserializationSchema(
                TableSchema.builder()
                        .field(NAME, DataTypes.STRING())
                        .field(COUNT, DataTypes.DECIMAL(38, 18))
                        .field(TIME, DataTypes.TIMESTAMP(3))
                        .build().toRowType()
        );

        final PscTableSourceBase expected = getExpectedPscTableSource(
                schema,
                Optional.of(PROC_TIME),
                rowtimeAttributeDescriptors,
                fieldMapping,
                TOPIC_URI,
                PSC_CONFIGURATION,
                deserializationSchema,
                StartupMode.SPECIFIC_OFFSETS,
                specificOffsets,
                0L);

        TableSourceValidation.validateTableSource(expected, schema);

        // construct table source using descriptors and table source factory
        final Map<String, String> propertiesMap = new HashMap<>();
        propertiesMap.putAll(createPscSourceProperties());
        propertiesMap.put("schema.watermark.0.rowtime", EVENT_TIME);
        propertiesMap.put("schema.watermark.0.strategy.expr", WATERMARK_EXPRESSION);
        propertiesMap.put("schema.watermark.0.strategy.data-type", WATERMARK_DATATYPE.toString());
        propertiesMap.put("schema.4.name", COMPUTED_COLUMN_NAME);
        propertiesMap.put("schema.4.data-type", COMPUTED_COLUMN_DATATYPE.toString());
        propertiesMap.put("schema.4.expr", COMPUTED_COLUMN_EXPRESSION);

        final TableSource<?> actualSource = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);

        assertEquals(expected, actualSource);

        // test PSC consumer
        final PscTableSourceBase actualPscSource = (PscTableSourceBase) actualSource;
        final StreamExecutionEnvironmentMock mock = new StreamExecutionEnvironmentMock();
        actualPscSource.getDataStream(mock);
        assertTrue(getExpectedFlinkPscConsumer().isAssignableFrom(mock.sourceFunction.getClass()));
        // Test commitOnCheckpoints flag should be true when set consumer group.
        assertTrue(((FlinkPscConsumerBase) mock.sourceFunction).getEnableCommitOnCheckpoints());
    }

    @Test
    public void testTableSourceCommitOnCheckpointsDisabled() {
        Map<String, String> propertiesMap = new HashMap<>();
        createPscSourceProperties().forEach((k, v) -> {
            if (!k.equals(PscValidator.CONNECTOR_PROPERTIES_GROUP_ID)) {
                propertiesMap.put(k, v);
            }
        });
        final TableSource<?> tableSource = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);
        final StreamExecutionEnvironmentMock mock = new StreamExecutionEnvironmentMock();
        // Test commitOnCheckpoints flag should be false when do not set consumer group.
        ((PscTableSourceBase) tableSource).getDataStream(mock);
        assertTrue(mock.sourceFunction instanceof FlinkPscConsumerBase);
        assertFalse(((FlinkPscConsumerBase) mock.sourceFunction).getEnableCommitOnCheckpoints());
    }

    @Test
    @SuppressWarnings("unchecked")
    @Ignore
    public void testTableSourceWithLegacyProperties() {
        // prepare parameters for Kafka table source
        final TableSchema schema = TableSchema.builder()
                .field(FRUIT_NAME, DataTypes.STRING())
                .field(COUNT, DataTypes.DECIMAL(38, 18))
                .field(EVENT_TIME, DataTypes.TIMESTAMP(3))
                .field(PROC_TIME, DataTypes.TIMESTAMP(3))
                .build();

        final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = Collections.singletonList(
                new RowtimeAttributeDescriptor(EVENT_TIME, new ExistingField(TIME), new AscendingTimestamps()));

        final Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put(FRUIT_NAME, NAME);
        fieldMapping.put(NAME, NAME);
        fieldMapping.put(COUNT, COUNT);
        fieldMapping.put(TIME, TIME);

        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_0), OFFSET_0);
        specificOffsets.put(new PscTopicUriPartition(TOPIC_URI, PARTITION_1), OFFSET_1);

        final TestDeserializationSchema deserializationSchema = new TestDeserializationSchema(
                TableSchema.builder()
                        .field(NAME, DataTypes.STRING())
                        .field(COUNT, DataTypes.DECIMAL(38, 18))
                        .field(TIME, DataTypes.TIMESTAMP(3))
                        .build().toRowType()
        );

        final PscTableSourceBase expected = getExpectedPscTableSource(
                schema,
                Optional.of(PROC_TIME),
                rowtimeAttributeDescriptors,
                fieldMapping,
                TOPIC_URI,
                PSC_CONFIGURATION,
                deserializationSchema,
                StartupMode.SPECIFIC_OFFSETS,
                specificOffsets,
                0L);

        TableSourceValidation.validateTableSource(expected, schema);

        // construct table source using descriptors and table source factory
        final Map<String, String> legacyPropertiesMap = new HashMap<>();
        legacyPropertiesMap.putAll(createPscSourceProperties());

        // use legacy properties
        legacyPropertiesMap.remove(PscValidator.CONNECTOR_SPECIFIC_OFFSETS);
        legacyPropertiesMap.remove(PscValidator.CONNECTOR_PROPERTIES_GROUP_ID);

        // keep compatible with a specified update-mode
        legacyPropertiesMap.put(StreamTableDescriptorValidator.UPDATE_MODE, "append");

        // legacy properties for specific-offsets and properties
        legacyPropertiesMap.put(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".0.partition", "0");
        legacyPropertiesMap.put(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".0.offset", "100");
        legacyPropertiesMap.put(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".1.partition", "1");
        legacyPropertiesMap.put(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".1.offset", "123");
        legacyPropertiesMap.put(PscValidator.CONNECTOR_PROPERTIES + ".0.key", PscConfiguration.PSC_CONSUMER_GROUP_ID);
        legacyPropertiesMap.put(PscValidator.CONNECTOR_PROPERTIES + ".0.value", "test");

        final TableSource<?> actualSource = TableFactoryService.find(StreamTableSourceFactory.class, legacyPropertiesMap)
                .createStreamTableSource(legacyPropertiesMap);

        assertEquals(expected, actualSource);

        // test Kafka consumer
        final PscTableSourceBase actualKafkaSource = (PscTableSourceBase) actualSource;
        final StreamExecutionEnvironmentMock mock = new StreamExecutionEnvironmentMock();
        actualKafkaSource.getDataStream(mock);
        assertTrue(getExpectedFlinkPscConsumer().isAssignableFrom(mock.sourceFunction.getClass()));
    }

    protected Map<String, String> createPscSourceProperties() {
        return new TestTableDescriptor(
                new Psc()
                        .version(getPscVersion())
                        .topicUri(TOPIC_URI)
                        .properties(PSC_CONFIGURATION)
                        .sinkPartitionerRoundRobin() // test if accepted although not needed
                        .startFromSpecificOffsets(OFFSETS))
                .withFormat(new TestTableFormat())
                .withSchema(
                        new Schema()
                                .field(FRUIT_NAME, DataTypes.STRING()).from(NAME)
                                .field(COUNT, DataTypes.DECIMAL(38, 18)) // no from so it must match with the input
                                .field(EVENT_TIME, DataTypes.TIMESTAMP(3)).rowtime(
                                new Rowtime().timestampsFromField(TIME).watermarksPeriodicAscending())
                                .field(PROC_TIME, DataTypes.TIMESTAMP(3)).proctime())
                .toProperties();
    }


    /**
     * This test can be unified with the corresponding source test once we have fixed FLINK-9870.
     */
    @Test
    public void testTableSink() {
        // prepare parameters for Kafka table sink
        final TableSchema schema = TableSchema.builder()
                .field(FRUIT_NAME, DataTypes.STRING())
                .field(COUNT, DataTypes.DECIMAL(10, 4))
                .field(EVENT_TIME, DataTypes.TIMESTAMP(3))
                .build();

        final PscTableSinkBase expected = getExpectedPscTableSink(
                schema,
                TOPIC_URI,
                PSC_CONFIGURATION,
                Optional.of(new FlinkFixedPartitioner<>()),
                new TestSerializationSchema(schema.toRowType()));

        // construct table sink using descriptors and table sink factory
        final Map<String, String> propertiesMap = createPscSinkProperties();
        final TableSink<?> actualSink = TableFactoryService.find(StreamTableSinkFactory.class, propertiesMap)
                .createStreamTableSink(propertiesMap);

        assertEquals(expected, actualSink);

        // test Kafka producer
        final PscTableSinkBase actualPscSink = (PscTableSinkBase) actualSink;
        final DataStreamMock streamMock = new DataStreamMock(new StreamExecutionEnvironmentMock(), schema.toRowType());
        actualPscSink.consumeDataStream(streamMock);
        assertTrue(getExpectedFlinkPscProducer().isAssignableFrom(streamMock.sinkFunction.getClass()));
    }

    @Test
    @Ignore
    public void testTableSinkWithLegacyProperties() {
        // prepare parameters for PSC table sink
        final TableSchema schema = TableSchema.builder()
                .field(FRUIT_NAME, DataTypes.STRING())
                .field(COUNT, DataTypes.DECIMAL(10, 4))
                .field(EVENT_TIME, DataTypes.TIMESTAMP(3))
                .build();

        final PscTableSinkBase expected = getExpectedPscTableSink(
                schema,
                TOPIC_URI,
                PSC_CONFIGURATION,
                Optional.of(new FlinkFixedPartitioner<>()),
                new TestSerializationSchema(schema.toRowType()));

        // construct table sink using descriptors and table sink factory
        final Map<String, String> legacyPropertiesMap = new HashMap<>();
        legacyPropertiesMap.putAll(createPscSinkProperties());

        // use legacy properties
        legacyPropertiesMap.remove(PscValidator.CONNECTOR_SPECIFIC_OFFSETS);
        legacyPropertiesMap.remove(PscValidator.CONNECTOR_PROPERTIES_GROUP_ID);

        // keep compatible with a specified update-mode
        legacyPropertiesMap.put(StreamTableDescriptorValidator.UPDATE_MODE, "append");

        // legacy properties for specific-offsets and properties
        legacyPropertiesMap.put(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".0.partition", "0");
        legacyPropertiesMap.put(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".0.offset", "100");
        legacyPropertiesMap.put(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".1.partition", "1");
        legacyPropertiesMap.put(PscValidator.CONNECTOR_SPECIFIC_OFFSETS + ".1.offset", "123");
        legacyPropertiesMap.put(PscValidator.CONNECTOR_PROPERTIES + ".0.key", PscConfiguration.PSC_CONSUMER_GROUP_ID);
        legacyPropertiesMap.put(PscValidator.CONNECTOR_PROPERTIES + ".0.value", "test");

        final TableSink<?> actualSink = TableFactoryService.find(StreamTableSinkFactory.class, legacyPropertiesMap)
                .createStreamTableSink(legacyPropertiesMap);

        assertEquals(expected, actualSink);

        // test PSC producer
        final PscTableSinkBase actualKafkaSink = (PscTableSinkBase) actualSink;
        final DataStreamMock streamMock = new DataStreamMock(new StreamExecutionEnvironmentMock(), schema.toRowType());
        actualKafkaSink.consumeDataStream(streamMock);
        assertTrue(getExpectedFlinkPscProducer().isAssignableFrom(streamMock.sinkFunction.getClass()));
    }

    protected Map<String, String> createPscSinkProperties() {
        return new TestTableDescriptor(
                new Psc()
                        .version(getPscVersion())
                        .topicUri(TOPIC_URI)
                        .properties(PSC_CONFIGURATION)
                        .sinkPartitionerFixed()
                        .startFromSpecificOffsets(OFFSETS)) // test if they accepted although not needed
                .withFormat(new TestTableFormat())
                .withSchema(
                        new Schema()
                                .field(FRUIT_NAME, DataTypes.STRING())
                                .field(COUNT, DataTypes.DECIMAL(10, 4))
                                .field(EVENT_TIME, DataTypes.TIMESTAMP(3)))
                .inAppendMode()
                .toProperties();
    }

    private static class StreamExecutionEnvironmentMock extends StreamExecutionEnvironment {

        public SourceFunction<?> sourceFunction;

        @Override
        public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> sourceFunction) {
            this.sourceFunction = sourceFunction;
            return super.addSource(sourceFunction);
        }

        @Override
        public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    private static class DataStreamMock extends DataStream<Row> {

        public SinkFunction<?> sinkFunction;

        public DataStreamMock(StreamExecutionEnvironment environment, TypeInformation<Row> outType) {
            super(environment, new TransformationMock("name", outType, 1));
        }

        @Override
        public DataStreamSink<Row> addSink(SinkFunction<Row> sinkFunction) {
            this.sinkFunction = sinkFunction;
            return super.addSink(sinkFunction);
        }
    }

    private static class TransformationMock extends Transformation<Row> {

        public TransformationMock(String name, TypeInformation<Row> outputType, int parallelism) {
            super(name, outputType, parallelism);
        }

        @Override
        public Collection<Transformation<?>> getTransitivePredecessors() {
            return null;
        }
    }

    // --------------------------------------------------------------------------------------------
    // For version-specific tests
    // --------------------------------------------------------------------------------------------

    protected abstract String getPscVersion();

    protected abstract Class<FlinkPscConsumerBase<Row>> getExpectedFlinkPscConsumer();

    protected abstract Class<?> getExpectedFlinkPscProducer();

    protected abstract PscTableSourceBase getExpectedPscTableSource(
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

    protected abstract PscTableSinkBase getExpectedPscTableSink(
            TableSchema schema,
            String topicUri,
            Properties properties,
            Optional<FlinkPscPartitioner<Row>> partitioner,
            SerializationSchema<Row> serializationSchema);
}
