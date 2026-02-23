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
     * Expected: Top-level column containing the nested field is passed to decoder.
     * The format (e.g., Thrift) handles extracting the specific nested field.
     *
     * Schema: a INT, b ROW<x STRING, y INT>, c BIGINT, d BOOLEAN
     * Query: SELECT b.x → needs top-level field b (index 1)
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

        // The decoder should receive the top-level field 'b' which contains the nested field
        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Collections.singletonList("b"));
    }

    /**
     * Test: SELECT a, b.y FROM table (mixed top-level and nested projection)
     * Expected: Top-level columns a and b are passed to decoder.
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

        // The decoder should receive both top-level fields a and b
        assertThat(decodedColumns)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList("a", "b"));
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
}
