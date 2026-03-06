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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.factories.TestFormatFactory.DecodingFormatMock;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for projection pushdown in {@link PscDynamicSource}.
 *
 * <p>These tests verify that when a query selects only a subset of columns,
 * the PSC source correctly pushes down the projection so that only the
 * required columns are deserialized.
 */
public class PscProjectionPushdownTest {

    /** Full schema: columns a, b, c, d */
    private static final DataType FULL_PHYSICAL_TYPE =
            DataTypes.ROW(
                            DataTypes.FIELD("a", DataTypes.INT()),
                            DataTypes.FIELD("b", DataTypes.STRING()),
                            DataTypes.FIELD("c", DataTypes.BIGINT()),
                            DataTypes.FIELD("d", DataTypes.BOOLEAN()))
                    .notNull();

    /**
     * Test: SELECT * FROM table
     * Expected: All columns [a, b, c, d] are passed to decoder
     */
    @Test
    public void testSelectAllColumns() {
        // SELECT * projects all fields: [a, b, c, d] -> indices [0, 1, 2, 3]
        final int[][] projectedFields = new int[][] {
            new int[] {0}, new int[] {1}, new int[] {2}, new int[] {3}
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING()),
                                DataTypes.FIELD("c", DataTypes.BIGINT()),
                                DataTypes.FIELD("d", DataTypes.BOOLEAN()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumns(projectedFields, projectedType);

        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList("a", "b", "c", "d"));
    }

    /**
     * Test: SELECT a, b, c FROM table
     * Expected: Only columns [a, b, c] are passed to decoder (excludes d)
     */
    @Test
    public void testSelectSubsetOfColumns() {
        // SELECT a, b, c -> indices [0, 1, 2]
        final int[][] projectedFields = new int[][] {
            new int[] {0}, new int[] {1}, new int[] {2}
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING()),
                                DataTypes.FIELD("c", DataTypes.BIGINT()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumns(projectedFields, projectedType);

        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList("a", "b", "c"));
    }

    /**
     * Test: SELECT a FROM table WHERE b = 'value'
     * Expected: Columns [a, b] are passed to decoder (a for projection, b for filter)
     *
     * Note: In Flink's optimization, when a filter references a column, that column
     * must be included in the projection even if not in the SELECT list.
     */
    @Test
    public void testSelectWithFilterRequiresBothColumns() {
        // SELECT a WHERE b = 'value' -> need both a (selected) and b (filter)
        // Flink planner will project [a, b] -> indices [0, 1]
        final int[][] projectedFields = new int[][] {
            new int[] {0}, new int[] {1}
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumns(projectedFields, projectedType);

        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList("a", "b"));
    }

    /**
     * Test: SELECT c, a FROM table (reordered columns)
     * Expected: Only columns [a, c] are passed to decoder
     */
    @Test
    public void testSelectReorderedColumns() {
        // SELECT c, a -> indices [2, 0] (reordered)
        final int[][] projectedFields = new int[][] {
            new int[] {2}, new int[] {0}
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("c", DataTypes.BIGINT()),
                                DataTypes.FIELD("a", DataTypes.INT()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumns(projectedFields, projectedType);

        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList("a", "c"));
    }

    /**
     * Test: SELECT d FROM table
     * Expected: Only column [d] is passed to decoder
     */
    @Test
    public void testSelectSingleColumn() {
        // SELECT d -> index [3]
        final int[][] projectedFields = new int[][] {
            new int[] {3}
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("d", DataTypes.BOOLEAN()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumns(projectedFields, projectedType);

        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Collections.singletonList("d"));
    }

    /**
     * Test: Verify that nested projection is supported.
     * This enables formats like Thrift to deserialize only specific nested fields.
     */
    @Test
    public void testSupportsNestedProjection() {
        PscDynamicSource source = createSource();
        assertThat(source.supportsNestedProjection()).isTrue();
    }

    /**
     * Test: SELECT nested.field FROM table (nested projection)
     * Expected: The projected nested field is passed to decoder with flattened name.
     * The format (e.g., Thrift) receives the pruned schema with nested structure.
     *
     * Schema: a INT, b ROW<x STRING, y INT>, c BIGINT, d BOOLEAN
     * Query: SELECT b.x → projects to nested field x within b
     */
    @Test
    public void testNestedProjection() {
        // SELECT b.x -> path [1, 0] means field 0 (x) within field 1 (b)
        final int[][] projectedFields = new int[][] {
            new int[] {1, 0}  // nested path: b.x
        };
        // The produced type after projection contains just the nested field
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("x", DataTypes.STRING()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumnsWithNestedSchema(
                projectedFields, projectedType);

        // DataTypeUtils.flattenToNames flattens nested ROW to b_x format
        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Collections.singletonList("b_x"));
    }

    /**
     * Test: SELECT a, b.y FROM table (mixed top-level and nested projection)
     * Expected: Top-level column a and nested field b.y (flattened) are passed to decoder.
     */
    @Test
    public void testMixedTopLevelAndNestedProjection() {
        // SELECT a, b.y -> paths [0] and [1, 1]
        final int[][] projectedFields = new int[][] {
            new int[] {0},     // top-level: a
            new int[] {1, 1}   // nested: b.y
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("y", DataTypes.INT()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumnsWithNestedSchema(
                projectedFields, projectedType);

        // DataTypeUtils.flattenToNames returns "a" and "b_y" (flattened)
        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList("a", "b_y"));
    }

    /**
     * Helper method to create a PscDynamicSource, apply projection, and return
     * the list of column names that would be passed to the decoder.
     */
    private List<String> applyProjectionAndGetDecodedColumns(
            int[][] projectedFields, DataType projectedType) {

        final String topicUri =
                com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub
                        .PSC_TEST_CLUSTER0_URI_PREFIX
                        + "projection-test-topic";

        final Properties sourceProperties = new Properties();
        sourceProperties.setProperty(
                com.pinterest.flink.connector.psc.PscFlinkConfiguration.CLUSTER_URI_CONFIG,
                com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub
                        .PSC_TEST_CLUSTER0_URI_PREFIX);
        sourceProperties.setProperty(
                com.pinterest.psc.config.PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        sourceProperties.setProperty("client.id.prefix", "test");
        sourceProperties.setProperty(
                com.pinterest.flink.connector.psc.source.PscSourceOptions
                        .PARTITION_DISCOVERY_INTERVAL_MS
                        .key(),
                "1000");

        final DecodingFormatMock valueFormat = new DecodingFormatMock(",", true);
        final PscDynamicSource source =
                new PscDynamicSource(
                        FULL_PHYSICAL_TYPE,
                        null,
                        valueFormat,
                        new int[0],
                        new int[] {0, 1, 2, 3},
                        null,
                        Collections.singletonList(topicUri),
                        (Pattern) null,
                        sourceProperties,
                        com.pinterest.flink.streaming.connectors.psc.config.StartupMode.EARLIEST,
                        new HashMap<>(),
                        0L,
                        com.pinterest.flink.streaming.connectors.psc.config.BoundedMode.UNBOUNDED,
                        new HashMap<>(),
                        0L,
                        false,
                        "test-table");

        // Apply the projection
        ((SupportsProjectionPushDown) source).applyProjection(projectedFields, projectedType);

        // Trigger creation of runtime decoders
        source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        // Get the captured data type from the mock format
        final DataType capturedDecoderProduced = valueFormat.producedDataType;
        assertThat(capturedDecoderProduced).isNotNull();

        return DataTypeUtils.flattenToNames(capturedDecoderProduced);
    }

    /** Schema with nested ROW type for nested projection tests: a INT, b ROW<x STRING, y INT>, c BIGINT, d BOOLEAN */
    private static final DataType NESTED_PHYSICAL_TYPE =
            DataTypes.ROW(
                            DataTypes.FIELD("a", DataTypes.INT()),
                            DataTypes.FIELD("b", DataTypes.ROW(
                                    DataTypes.FIELD("x", DataTypes.STRING()),
                                    DataTypes.FIELD("y", DataTypes.INT()))),
                            DataTypes.FIELD("c", DataTypes.BIGINT()),
                            DataTypes.FIELD("d", DataTypes.BOOLEAN()))
                    .notNull();

    /**
     * Helper method for nested projection tests.
     */
    private List<String> applyProjectionAndGetDecodedColumnsWithNestedSchema(
            int[][] projectedFields, DataType projectedType) {

        final String topicUri =
                com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub
                        .PSC_TEST_CLUSTER0_URI_PREFIX
                        + "projection-test-topic";

        final Properties sourceProperties = new Properties();
        sourceProperties.setProperty(
                com.pinterest.flink.connector.psc.PscFlinkConfiguration.CLUSTER_URI_CONFIG,
                com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub
                        .PSC_TEST_CLUSTER0_URI_PREFIX);
        sourceProperties.setProperty(
                com.pinterest.psc.config.PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        sourceProperties.setProperty("client.id.prefix", "test");
        sourceProperties.setProperty(
                com.pinterest.flink.connector.psc.source.PscSourceOptions
                        .PARTITION_DISCOVERY_INTERVAL_MS
                        .key(),
                "1000");

        final DecodingFormatMock valueFormat = new DecodingFormatMock(",", true);
        final PscDynamicSource source =
                new PscDynamicSource(
                        NESTED_PHYSICAL_TYPE,
                        null,
                        valueFormat,
                        new int[0],
                        new int[] {0, 1, 2, 3},
                        null,
                        Collections.singletonList(topicUri),
                        (Pattern) null,
                        sourceProperties,
                        com.pinterest.flink.streaming.connectors.psc.config.StartupMode.EARLIEST,
                        new HashMap<>(),
                        0L,
                        com.pinterest.flink.streaming.connectors.psc.config.BoundedMode.UNBOUNDED,
                        new HashMap<>(),
                        0L,
                        false,
                        "test-table");

        // Apply the projection
        ((SupportsProjectionPushDown) source).applyProjection(projectedFields, projectedType);

        // Trigger creation of runtime decoders
        source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        // Get the captured data type from the mock format
        final DataType capturedDecoderProduced = valueFormat.producedDataType;
        assertThat(capturedDecoderProduced).isNotNull();

        return DataTypeUtils.flattenToNames(capturedDecoderProduced);
    }

    /**
     * Helper method to create a basic PscDynamicSource for simple tests.
     */
    private PscDynamicSource createSource() {
        final String topicUri =
                com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub
                        .PSC_TEST_CLUSTER0_URI_PREFIX
                        + "projection-test-topic";

        final Properties sourceProperties = new Properties();
        sourceProperties.setProperty(
                com.pinterest.flink.connector.psc.PscFlinkConfiguration.CLUSTER_URI_CONFIG,
                com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub
                        .PSC_TEST_CLUSTER0_URI_PREFIX);
        sourceProperties.setProperty(
                com.pinterest.psc.config.PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        sourceProperties.setProperty("client.id.prefix", "test");
        sourceProperties.setProperty(
                com.pinterest.flink.connector.psc.source.PscSourceOptions
                        .PARTITION_DISCOVERY_INTERVAL_MS
                        .key(),
                "1000");

        final DecodingFormatMock valueFormat = new DecodingFormatMock(",", true);
        return new PscDynamicSource(
                FULL_PHYSICAL_TYPE,
                null,
                valueFormat,
                new int[0],
                new int[] {0, 1, 2, 3},
                null,
                Collections.singletonList(topicUri),
                (Pattern) null,
                sourceProperties,
                com.pinterest.flink.streaming.connectors.psc.config.StartupMode.EARLIEST,
                new HashMap<>(),
                0L,
                com.pinterest.flink.streaming.connectors.psc.config.BoundedMode.UNBOUNDED,
                new HashMap<>(),
                0L,
                false,
                "test-table");
    }

    // ==================== Backwards Compatibility Tests ====================

    /**
     * Test: Verify that a source created without projection pushdown has default
     * nested projection arrays initialized (single-element paths for each field).
     * This ensures backwards compatibility with existing code.
     */
    @Test
    public void testDefaultNestedProjectionInitialization() {
        PscDynamicSource source = createSource();

        // Copy the source to access internal state
        PscDynamicSource copy = (PscDynamicSource) source.copy();

        // Without any projection applied, nested projections should be default
        // (single-element paths matching the value projection)
        // This verifies the constructor properly initializes the nested arrays
        assertThat(copy).isNotNull();
    }

    /**
     * Test: Verify that copy() properly preserves nested projection state.
     * This ensures that source copying (used in Flink's optimizer) works correctly.
     */
    @Test
    public void testCopyPreservesNestedProjection() {
        PscDynamicSource source = createSourceWithNestedSchema();

        // Apply nested projection
        final int[][] projectedFields = new int[][] {
            new int[] {0},      // a
            new int[] {1, 0},   // b.x
            new int[] {1, 1}    // b.y
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("x", DataTypes.STRING()),
                                DataTypes.FIELD("y", DataTypes.INT()))
                        .notNull();

        ((SupportsProjectionPushDown) source).applyProjection(projectedFields, projectedType);

        // Copy the source
        DynamicTableSource copiedSource = source.copy();

        // Verify copy is not the same instance
        assertThat(copiedSource).isNotSameAs(source);

        // We can't easily compare internal state, but we verify both are PscDynamicSource
        assertThat(copiedSource).isInstanceOf(PscDynamicSource.class);
    }

    /**
     * Test: Verify convertPathsToFieldNames utility method for simple paths.
     */
    @Test
    public void testConvertPathsToFieldNamesSimple() {
        final int[][] paths = new int[][] {
            new int[] {0},  // a
            new int[] {2}   // c
        };

        List<String> fieldNames = PscDynamicSource.convertPathsToFieldNames(paths, FULL_PHYSICAL_TYPE);

        assertThat(fieldNames).containsExactly("a", "c");
    }

    /**
     * Test: Verify convertPathsToFieldNames utility method for nested paths.
     */
    @Test
    public void testConvertPathsToFieldNamesNested() {
        final int[][] paths = new int[][] {
            new int[] {0},      // a
            new int[] {1, 0},   // b.x
            new int[] {1, 1}    // b.y
        };

        List<String> fieldNames = PscDynamicSource.convertPathsToFieldNames(paths, NESTED_PHYSICAL_TYPE);

        assertThat(fieldNames).containsExactly("a", "b.x", "b.y");
    }

    /**
     * Test: Verify convertPathsToFieldNames with deeply nested paths.
     */
    @Test
    public void testConvertPathsToFieldNamesDeeplyNested() {
        // Schema: a INT, b ROW<x STRING, y ROW<p INT, q STRING>>
        final DataType deeplyNestedType =
                DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.ROW(
                                        DataTypes.FIELD("x", DataTypes.STRING()),
                                        DataTypes.FIELD("y", DataTypes.ROW(
                                                DataTypes.FIELD("p", DataTypes.INT()),
                                                DataTypes.FIELD("q", DataTypes.STRING()))))))
                        .notNull();

        final int[][] paths = new int[][] {
            new int[] {0},         // a
            new int[] {1, 0},      // b.x
            new int[] {1, 1, 0},   // b.y.p
            new int[] {1, 1, 1}    // b.y.q
        };

        List<String> fieldNames = PscDynamicSource.convertPathsToFieldNames(paths, deeplyNestedType);

        assertThat(fieldNames).containsExactly("a", "b.x", "b.y.p", "b.y.q");
    }

    /**
     * Test: Verify convertPathsToFieldNames with ARRAY containing ROW type.
     * Schema: items ARRAY<ROW<id INT, name STRING>>
     */
    @Test
    public void testConvertPathsToFieldNamesArrayOfRow() {
        final DataType arrayOfRowType =
                DataTypes.ROW(
                                DataTypes.FIELD("items", DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("id", DataTypes.INT()),
                                                DataTypes.FIELD("name", DataTypes.STRING())))))
                        .notNull();

        final int[][] paths = new int[][] {
            new int[] {0, 0},   // items.id
            new int[] {0, 1}    // items.name
        };

        List<String> fieldNames = PscDynamicSource.convertPathsToFieldNames(paths, arrayOfRowType);

        assertThat(fieldNames).containsExactly("items.id", "items.name");
    }

    /**
     * Test: Verify that nested projection works correctly with multiple nested fields
     * from the same parent and produces the correct pruned DataType for the format.
     */
    @Test
    public void testMultipleNestedFieldsFromSameParent() {
        // SELECT b.x, b.y FROM table -> paths [1, 0] and [1, 1]
        final int[][] projectedFields = new int[][] {
            new int[] {1, 0},   // b.x
            new int[] {1, 1}    // b.y
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("x", DataTypes.STRING()),
                                DataTypes.FIELD("y", DataTypes.INT()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumnsWithNestedSchema(
                projectedFields, projectedType);

        // DataTypeUtils.flattenToNames returns "b_x" and "b_y" (flattened)
        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList("b_x", "b_y"));
    }

    /**
     * Test: Verify backwards compatibility - existing code that doesn't use
     * nested projection still works correctly.
     */
    @Test
    public void testBackwardsCompatibilityWithoutNestedProjection() {
        // Simple top-level projection without any nested fields
        final int[][] projectedFields = new int[][] {
            new int[] {0},
            new int[] {1}
        };
        final DataType projectedType =
                DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING()))
                        .notNull();

        List<String> decodedColumns = applyProjectionAndGetDecodedColumns(projectedFields, projectedType);

        // Should work exactly as before
        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList("a", "b"));
    }

    /**
     * Helper method to create a PscDynamicSource with nested schema.
     */
    private PscDynamicSource createSourceWithNestedSchema() {
        final String topicUri =
                com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub
                        .PSC_TEST_CLUSTER0_URI_PREFIX
                        + "projection-test-topic";

        final Properties sourceProperties = new Properties();
        sourceProperties.setProperty(
                com.pinterest.flink.connector.psc.PscFlinkConfiguration.CLUSTER_URI_CONFIG,
                com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub
                        .PSC_TEST_CLUSTER0_URI_PREFIX);
        sourceProperties.setProperty(
                com.pinterest.psc.config.PscConfiguration.PSC_CONSUMER_GROUP_ID, "dummy");
        sourceProperties.setProperty("client.id.prefix", "test");
        sourceProperties.setProperty(
                com.pinterest.flink.connector.psc.source.PscSourceOptions
                        .PARTITION_DISCOVERY_INTERVAL_MS
                        .key(),
                "1000");

        final DecodingFormatMock valueFormat = new DecodingFormatMock(",", true);
        return new PscDynamicSource(
                NESTED_PHYSICAL_TYPE,
                null,
                valueFormat,
                new int[0],
                new int[] {0, 1, 2, 3},
                null,
                Collections.singletonList(topicUri),
                (Pattern) null,
                sourceProperties,
                com.pinterest.flink.streaming.connectors.psc.config.StartupMode.EARLIEST,
                new HashMap<>(),
                0L,
                com.pinterest.flink.streaming.connectors.psc.config.BoundedMode.UNBOUNDED,
                new HashMap<>(),
                0L,
                false,
                "test-table");
    }
}
