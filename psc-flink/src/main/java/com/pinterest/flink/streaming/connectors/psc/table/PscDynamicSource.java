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

import com.pinterest.flink.connector.psc.source.PscSource;
import com.pinterest.flink.connector.psc.source.PscSourceBuilder;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.streaming.connectors.psc.PscDeserializationSchema;
import com.pinterest.flink.streaming.connectors.psc.config.BoundedMode;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_STARTUP_MODE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** A version-agnostic PSC {@link ScanTableSource}. */
@Internal
public class PscDynamicSource
        implements
                ScanTableSource,
                SupportsReadingMetadata,
                SupportsWatermarkPushDown,
                SupportsProjectionPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(PscDynamicSource.class);

    private static final String PSC_TRANSFORMATION = "psc";

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    /** Watermark strategy that is used to generate per-partition watermark. */
    protected @Nullable WatermarkStrategy<RowData> watermarkStrategy;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    private static final String VALUE_METADATA_PREFIX = "value.";

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /** Optional format for decoding keys from PSC. */
    protected final @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

    /** Format for decoding values from PSC. */
    protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    /** Indices that determine the key fields and the target position in the produced row. */
    protected final int[] keyProjection;

    /** Indices that determine the value fields and the target position in the produced row. */
    protected final int[] valueProjection;

    // Query-specific projections (see SupportsProjectionPushDown).
    // - *Format* projections are indices into physicalDataType (control what gets deserialized).
    // - *Output* projections are indices into the projected physical row (control where fields land).
    protected int[] keyFormatProjection;
    protected int[] valueFormatProjection;
    protected int[] keyOutputProjection;
    protected int[] valueOutputProjection;

    // Full nested projection paths for each format field.
    // Each int[] is a path: [topLevelIndex] for top-level, [topLevelIndex, nestedIndex, ...] for nested.
    // Used by formats that support nested projection (e.g., Thrift's PartialThriftDeserializer).
    protected int[][] keyNestedProjection;
    protected int[][] valueNestedProjection;

    /** Prefix that needs to be removed from fields when constructing the physical data type. */
    protected final @Nullable String keyPrefix;

    // --------------------------------------------------------------------------------------------
    // PSC-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The PSC topics to consume. */
    protected final List<String> topicUris;

    /** The PSC topic pattern to consume. */
    protected final Pattern topicUriPattern;

    /** Properties for the PSC consumer. */
    protected final Properties properties;

    /**
     * The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}).
     */
    protected final StartupMode startupMode;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link
     * StartupMode#SPECIFIC_OFFSETS}.
     */
    protected final Map<PscTopicUriPartition, Long> specificStartupOffsets;

    /**
     * The start timestamp to locate partition offsets; only relevant when startup mode is {@link
     * StartupMode#TIMESTAMP}.
     */
    protected final long startupTimestampMillis;

    /** The bounded mode for the contained consumer (default is an unbounded data stream). */
    protected final BoundedMode boundedMode;

    /**
     * Specific end offsets; only relevant when bounded mode is {@link
     * BoundedMode#SPECIFIC_OFFSETS}.
     */
    protected final Map<PscTopicUriPartition, Long> specificBoundedOffsets;

    /**
     * The bounded timestamp to locate partition offsets; only relevant when bounded mode is {@link
     * BoundedMode#TIMESTAMP}.
     */
    protected final long boundedTimestampMillis;

    /** Flag to determine source mode. In upsert mode, it will keep the tombstone message. * */
    protected final boolean upsertMode;

    protected final String tableIdentifier;

    /** Optional user-provided UID prefix for stabilizing operator UIDs across DAG changes. */
    protected final @Nullable String sourceUidPrefix;

    /** Enable rescale() shuffle to redistribute data across downstream operators. */
    protected final boolean enableRescale;

    /** Optional rate limit in records per second. */
    protected final @Nullable Double rateLimitRecordsPerSecond;

    /** Optional explicit source parallelism from scan.parallelism configuration. */
    protected final @Nullable Integer scanParallelism;

    /**
     * Checks if rate limiting is enabled.
     * 
     * @param rateLimitRecordsPerSecond the rate limit configuration value
     * @return true if rate limiting is enabled (not null and > 0), false otherwise
     */
    public static boolean isRateLimitingEnabled(@Nullable Double rateLimitRecordsPerSecond) {
        return rateLimitRecordsPerSecond != null && rateLimitRecordsPerSecond > 0;
    }

    /**
     * Determines the intended downstream parallelism.
     * Uses scan.parallelism if configured, otherwise falls back to global default.
     * 
     * @param execEnv the stream execution environment
     * @return the intended parallelism for downstream operators
     */
    private int getIntendedParallelism(StreamExecutionEnvironment execEnv) {
        return scanParallelism != null ? scanParallelism : execEnv.getParallelism();
    }

    public PscDynamicSource(
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
            long startupTimestampMillis,
            BoundedMode boundedMode,
            Map<PscTopicUriPartition, Long> specificBoundedOffsets,
            long boundedTimestampMillis,
            boolean upsertMode,
            String tableIdentifier,
            @Nullable String sourceUidPrefix,
            boolean enableRescale,
            @Nullable Double rateLimitRecordsPerSecond,
            @Nullable Integer scanParallelism) {
        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.keyDecodingFormat = keyDecodingFormat;
        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");
        this.keyProjection =
                Preconditions.checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection =
                Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;

        // Default behavior: no projection pushdown, keep the DDL-level projections.
        this.keyFormatProjection = this.keyProjection;
        this.valueFormatProjection = this.valueProjection;
        this.keyOutputProjection = this.keyProjection;
        this.valueOutputProjection = this.valueProjection;
        // Default nested projections: single-element paths for each top-level field
        this.keyNestedProjection = toNestedProjection(this.keyProjection);
        this.valueNestedProjection = toNestedProjection(this.valueProjection);
        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.metadataKeys = Collections.emptyList();
        this.watermarkStrategy = null;
        // PSC-specific attributes
        Preconditions.checkArgument(
                (topics != null && topicPattern == null)
                        || (topics == null && topicPattern != null),
                "Either Topic or Topic Pattern must be set for source.");
        this.topicUris = topics;
        this.topicUriPattern = topicPattern;
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.startupMode =
                Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
        this.specificStartupOffsets =
                Preconditions.checkNotNull(
                        specificStartupOffsets, "Specific offsets must not be null.");
        this.startupTimestampMillis = startupTimestampMillis;
        this.boundedMode =
                Preconditions.checkNotNull(boundedMode, "Bounded mode must not be null.");
        this.specificBoundedOffsets =
                Preconditions.checkNotNull(
                        specificBoundedOffsets, "Specific bounded offsets must not be null.");
        this.boundedTimestampMillis = boundedTimestampMillis;
        this.upsertMode = upsertMode;
        this.tableIdentifier = tableIdentifier;
        this.sourceUidPrefix = sourceUidPrefix;
        this.enableRescale = enableRescale;
        this.rateLimitRecordsPerSecond = rateLimitRecordsPerSecond;
        this.scanParallelism = scanParallelism;
    }

    /**
     * Backward-compatible constructor without UID prefix and rescale. Delegates to the full
     * constructor with null {@code sourceUidPrefix} and false {@code enableRescale}.
     */
    public PscDynamicSource(
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
            long startupTimestampMillis,
            BoundedMode boundedMode,
            Map<PscTopicUriPartition, Long> specificBoundedOffsets,
            long boundedTimestampMillis,
            boolean upsertMode,
            String tableIdentifier) {
        this(
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
                boundedMode,
                specificBoundedOffsets,
                boundedTimestampMillis,
                upsertMode,
                tableIdentifier,
                null,
                false,
                null,
                null);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(
                        context,
                        keyDecodingFormat,
                        keyFormatProjection,
                        keyNestedProjection,
                        keyPrefix);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(
                        context,
                        valueDecodingFormat,
                        valueFormatProjection,
                        valueNestedProjection,
                        null);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final PscSource<RowData> pscSource =
                createPscSource(keyDeserialization, valueDeserialization, producedTypeInfo);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                if (watermarkStrategy == null) {
                    watermarkStrategy = WatermarkStrategy.noWatermarks();
                }
                DataStreamSource<RowData> sourceStream =
                        execEnv.fromSource(
                                pscSource, watermarkStrategy, "PscSource-" + tableIdentifier);
                
                // Source parallelism is determined by partition count (Flink's default for Kafka-like sources)
                // We do NOT set it explicitly even if scan.parallelism is configured, because:
                // - A Kafka source can only have as many active subtasks as there are partitions
                // - Setting higher parallelism would create idle subtasks
                // - Instead, we use rescale() to redistribute data to the intended downstream parallelism
                
                DataStream<RowData> resultStream = sourceStream;
                
                // Determine the intended downstream parallelism for rate limiting
                // This is scan.parallelism if set, otherwise global default parallelism
                int intendedParallelism = getIntendedParallelism(execEnv);
                
                // Apply rescale FIRST if enabled
                // This redistributes data from source parallelism (= partition count) to intended parallelism
                // Ensures all downstream subtasks (including rate limiters) receive traffic
                if (enableRescale) {
                    resultStream = resultStream.rescale();
                }
                
                // Apply rate limiting AFTER rescale if configured
                // Rate limiter parallelism must match the actual parallelism of incoming data:
                // - If rescale enabled: use intendedParallelism (all subtasks are active after rescale)
                // - If rescale disabled: use source parallelism (rate limiter stays with source)
                if (isRateLimitingEnabled(rateLimitRecordsPerSecond)) {
                    int rateLimiterParallelism = enableRescale ? intendedParallelism : sourceStream.getParallelism();
                    
                    String rateLimiterOperatorName = "PscRateLimit-" + tableIdentifier;
                    resultStream = resultStream
                            .map(new PscRateLimitMap<>(rateLimitRecordsPerSecond))
                            .setParallelism(rateLimiterParallelism)
                            .name(rateLimiterOperatorName)
                            .uid(rateLimiterOperatorName);
                }
                
                // Prefer explicit user-provided UID prefix if present; otherwise rely on provider context.
                if (sourceUidPrefix != null) {
                    final String trimmedPrefix = sourceUidPrefix.trim();
                    if (!trimmedPrefix.isEmpty()) {
                        sourceStream.uid(trimmedPrefix + ":" + PSC_TRANSFORMATION + ":" + tableIdentifier);
                        return resultStream;
                    }
                }
                providerContext.generateUid(PSC_TRANSFORMATION).ifPresent(sourceStream::uid);
                return resultStream;
            }

            @Override
            public boolean isBounded() {
                return pscSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence

        // add value format metadata with prefix
        valueDecodingFormat
                .listReadableMetadata()
                .forEach((key, value) -> metadataMap.put(VALUE_METADATA_PREFIX + key, value));

        // add connector metadata
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // separate connector and format metadata
        final List<String> formatMetadataKeys =
                metadataKeys.stream()
                        .filter(k -> k.startsWith(VALUE_METADATA_PREFIX))
                        .collect(Collectors.toList());
        final List<String> connectorMetadataKeys = new ArrayList<>(metadataKeys);
        connectorMetadataKeys.removeAll(formatMetadataKeys);

        // push down format metadata
        final Map<String, DataType> formatMetadata = valueDecodingFormat.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            final List<String> requestedFormatMetadataKeys =
                    formatMetadataKeys.stream()
                            .map(k -> k.substring(VALUE_METADATA_PREFIX.length()))
                            .collect(Collectors.toList());
            valueDecodingFormat.applyReadableMetadata(requestedFormatMetadataKeys);
        }

        this.metadataKeys = connectorMetadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public boolean supportsMetadataProjection() {
        return false;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public boolean supportsNestedProjection() {
        return true;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        Preconditions.checkNotNull(projectedFields, "Projected fields must not be null.");
        Preconditions.checkNotNull(producedDataType, "Produced data type must not be null.");

        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);

        // projectedFields is a 2D array where:
        // - The first dimension represents the output field position (order in the projected row)
        // - The second dimension is the path to the field: [topLevelIndex] for top-level fields,
        //   or [topLevelIndex, nestedIndex, ...] for nested fields within ROW types
        // Example: For schema (a INT, b ROW<x STRING, y INT>, c BIGINT):
        //   - [[2], [1, 0]] means SELECT c, b.x → output row has c at position 0, b.x at position 1
        //   - [2] is the path to top-level field 'c'
        //   - [1, 0] is the path to nested field 'x' within 'b'

        // Build mapping from top-level physical index to output position
        final int[] physicalIndexToOutputIndex = new int[physicalFieldCount];
        Arrays.fill(physicalIndexToOutputIndex, -1);

        // Group projected paths by their top-level field index
        // This allows us to track all nested paths for each top-level field
        final Map<Integer, List<int[]>> pathsByTopLevelIndex = new LinkedHashMap<>();

        for (int outputPos = 0; outputPos < projectedFields.length; outputPos++) {
            final int[] path = projectedFields[outputPos];
            Preconditions.checkArgument(
                    path != null && path.length >= 1,
                    "Projection path must have at least one element but got: %s",
                    Arrays.toString(path));
            final int physicalPos = path[0];
            Preconditions.checkArgument(
                    physicalPos >= 0 && physicalPos < physicalFieldCount,
                    "Projected field index out of bounds: %s",
                    physicalPos);
            physicalIndexToOutputIndex[physicalPos] = outputPos;

            // Collect all paths for this top-level field
            pathsByTopLevelIndex.computeIfAbsent(physicalPos, k -> new ArrayList<>()).add(path);
        }

        // This sets the physical output type. Note that SupportsReadingMetadata#applyReadableMetadata
        // may overwrite producedDataType later with appended metadata columns.
        this.producedDataType = producedDataType;

        // Build key format projection and nested paths
        List<Integer> keyFormatList = new ArrayList<>();
        List<int[]> keyNestedList = new ArrayList<>();
        for (int physicalPos : keyProjection) {
            if (physicalIndexToOutputIndex[physicalPos] >= 0) {
                keyFormatList.add(physicalPos);
                List<int[]> paths = pathsByTopLevelIndex.get(physicalPos);
                if (paths != null) {
                    keyNestedList.addAll(paths);
                }
            }
        }
        this.keyFormatProjection = keyFormatList.stream().mapToInt(Integer::intValue).toArray();
        this.keyNestedProjection = keyNestedList.toArray(new int[0][]);

        // Build value format projection and nested paths
        List<Integer> valueFormatList = new ArrayList<>();
        List<int[]> valueNestedList = new ArrayList<>();
        for (int physicalPos : valueProjection) {
            if (physicalIndexToOutputIndex[physicalPos] >= 0) {
                valueFormatList.add(physicalPos);
                List<int[]> paths = pathsByTopLevelIndex.get(physicalPos);
                if (paths != null) {
                    valueNestedList.addAll(paths);
                }
            }
        }
        this.valueFormatProjection = valueFormatList.stream().mapToInt(Integer::intValue).toArray();
        this.valueNestedProjection = valueNestedList.toArray(new int[0][]);

        // Remap decoded fields into the projected output row order.
        this.keyOutputProjection =
                IntStream.of(this.keyFormatProjection)
                        .map(physicalPos -> physicalIndexToOutputIndex[physicalPos])
                        .toArray();
        this.valueOutputProjection =
                IntStream.of(this.valueFormatProjection)
                        .map(physicalPos -> physicalIndexToOutputIndex[physicalPos])
                        .toArray();
    }

    /** Converts a 1D projection (top-level indices) to 2D nested projection format. */
    private static int[][] toNestedProjection(int[] projection) {
        int[][] result = new int[projection.length][];
        for (int i = 0; i < projection.length; i++) {
            result[i] = new int[] {projection[i]};
        }
        return result;
    }

    /**
     * Converts nested projection paths to dot-separated field names.
     * Example: [[1, 0], [2]] with schema (a, b ROW&lt;x, y&gt;, c) → ["b.x", "c"]
     */
    public static List<String> convertPathsToFieldNames(int[][] paths, DataType dataType) {
        List<String> fieldNames = new ArrayList<>();
        List<String> topLevelNames = DataType.getFieldNames(dataType);
        List<DataType> topLevelTypes = DataType.getFieldDataTypes(dataType);

        for (int[] path : paths) {
            StringBuilder name = new StringBuilder();
            List<String> currentNames = topLevelNames;
            List<DataType> currentTypes = topLevelTypes;

            for (int i = 0; i < path.length; i++) {
                int index = path[i];
                if (i > 0) {
                    name.append(".");
                }
                name.append(currentNames.get(index));

                // Navigate to nested type for next iteration
                if (i < path.length - 1) {
                    DataType nestedType = currentTypes.get(index);
                    // Unwrap collection types (ARRAY, MAP) to get to element/value type
                    nestedType = unwrapCollectionType(nestedType);
                    currentNames = DataType.getFieldNames(nestedType);
                    currentTypes = DataType.getFieldDataTypes(nestedType);
                }
            }
            fieldNames.add(name.toString());
        }
        return fieldNames;
    }

    /**
     * Unwraps collection types (ARRAY, MAP) to get the element/value type containing ROW fields.
     */
    private static DataType unwrapCollectionType(DataType dataType) {
        LogicalType logicalType = dataType.getLogicalType();
        if (logicalType instanceof ArrayType) {
            // ARRAY<ROW<...>> - get the element type
            List<DataType> children = dataType.getChildren();
            if (!children.isEmpty()) {
                return children.get(0);
            }
        } else if (logicalType instanceof org.apache.flink.table.types.logical.MapType) {
            // MAP<K, ROW<...>> - get the value type (second child)
            List<DataType> children = dataType.getChildren();
            if (children.size() >= 2) {
                return children.get(1);
            }
        }
        return dataType;
    }

    @Override
    public DynamicTableSource copy() {
        final PscDynamicSource copy =
                new PscDynamicSource(
                        physicalDataType,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        topicUris,
                        topicUriPattern,
                        properties,
                        startupMode,
                        specificStartupOffsets,
                        startupTimestampMillis,
                        boundedMode,
                        specificBoundedOffsets,
                        boundedTimestampMillis,
                        upsertMode,
                        tableIdentifier,
                        sourceUidPrefix,
                        enableRescale,
                        rateLimitRecordsPerSecond,
                        scanParallelism);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;
        copy.keyFormatProjection = keyFormatProjection;
        copy.valueFormatProjection = valueFormatProjection;
        copy.keyOutputProjection = keyOutputProjection;
        copy.valueOutputProjection = valueOutputProjection;
        copy.keyNestedProjection = keyNestedProjection;
        copy.valueNestedProjection = valueNestedProjection;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "PSC table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PscDynamicSource that = (PscDynamicSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyDecodingFormat, that.keyDecodingFormat)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(topicUris, that.topicUris)
                && Objects.equals(String.valueOf(topicUriPattern), String.valueOf(that.topicUriPattern))
                && Objects.equals(properties, that.properties)
                && startupMode == that.startupMode
                && Objects.equals(specificStartupOffsets, that.specificStartupOffsets)
                && startupTimestampMillis == that.startupTimestampMillis
                && boundedMode == that.boundedMode
                && Objects.equals(specificBoundedOffsets, that.specificBoundedOffsets)
                && boundedTimestampMillis == that.boundedTimestampMillis
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(tableIdentifier, that.tableIdentifier)
                && Objects.equals(sourceUidPrefix, that.sourceUidPrefix)
                && enableRescale == that.enableRescale
                && Objects.equals(rateLimitRecordsPerSecond, that.rateLimitRecordsPerSecond)
                && Objects.equals(scanParallelism, that.scanParallelism)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                metadataKeys,
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                Arrays.hashCode(keyProjection),
                Arrays.hashCode(valueProjection),
                keyPrefix,
                topicUris,
                topicUriPattern,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                boundedMode,
                specificBoundedOffsets,
                boundedTimestampMillis,
                upsertMode,
                tableIdentifier,
                sourceUidPrefix,
                enableRescale,
                rateLimitRecordsPerSecond,
                scanParallelism,
                watermarkStrategy);
    }

    // --------------------------------------------------------------------------------------------

    protected PscSource<RowData> createPscSource(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final PscDeserializationSchema<RowData> pscDeserializer =
                createPscDeserializationSchema(
                        keyDeserialization, valueDeserialization, producedTypeInfo);

        final PscSourceBuilder<RowData> pscSourceBuilder = PscSource.builder();

        if (topicUris != null) {
            pscSourceBuilder.setTopicUris(topicUris);
        } else {
            pscSourceBuilder.setTopicUriPattern(topicUriPattern);
        }

        LOG.info("{}: {}", SCAN_STARTUP_MODE, startupMode);
        switch (startupMode) {
            case EARLIEST:
                pscSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
                LOG.info("Setting starting offsets to earliest");
                break;
            case LATEST:
                pscSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
                LOG.info("Setting starting offsets to latest");
                break;
            case GROUP_OFFSETS:
                String offsetResetConfig =
                        properties.getProperty(
                                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET,
                                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE);
                offsetResetConfig = getResetStrategy(offsetResetConfig);
                pscSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.committedOffsets(offsetResetConfig));
                LOG.info("Setting starting offsets to committed offsets with reset strategy: {}", offsetResetConfig);
                break;
            case SPECIFIC_OFFSETS:
                Map<TopicUriPartition, Long> offsets = new HashMap<>();
                specificStartupOffsets.forEach(
                        (tp, offset) ->
                                offsets.put(
                                        new TopicUriPartition(tp.getTopicUriStr(), tp.getPartition()),
                                        offset));
                pscSourceBuilder.setStartingOffsets(OffsetsInitializer.offsets(offsets));
                LOG.info("Setting starting offsets to specific offsets: {}", offsets);
                break;
            case TIMESTAMP:
                pscSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.timestamp(startupTimestampMillis));
                LOG.info("Setting starting offsets to timestamp: {}", startupTimestampMillis);
                break;
        }

        switch (boundedMode) {
            case UNBOUNDED:
                pscSourceBuilder.setUnbounded(new NoStoppingOffsetsInitializer());
                break;
            case LATEST:
                pscSourceBuilder.setBounded(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                pscSourceBuilder.setBounded(OffsetsInitializer.committedOffsets());
                break;
            case SPECIFIC_OFFSETS:
                Map<TopicUriPartition, Long> offsets = new HashMap<>();
                specificBoundedOffsets.forEach(
                        (tp, offset) ->
                                offsets.put(
                                        new TopicUriPartition(tp.getTopicUri(), tp.getPartition()),
                                        offset));
                pscSourceBuilder.setBounded(OffsetsInitializer.offsets(offsets));
                break;
            case TIMESTAMP:
                pscSourceBuilder.setBounded(OffsetsInitializer.timestamp(boundedTimestampMillis));
                break;
        }

        pscSourceBuilder
                .setProperties(properties)
                .setDeserializer(PscRecordDeserializationSchema.of(pscDeserializer));

        return pscSourceBuilder.build();
    }

    private String getResetStrategy(String offsetResetConfig) {
        final String[] validResetStrategies = {"EARLIEST", "LATEST", "NONE"};
        return Arrays.stream(validResetStrategies)
                .filter(ors -> ors.equals(offsetResetConfig.toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "%s can not be set to %s. Valid values: [%s]",
                                                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET,
                                                offsetResetConfig,
                                                Arrays.stream(validResetStrategies)
                                                        .map(String::toLowerCase)
                                                        .collect(Collectors.joining(",")))));
    }

    private PscDeserializationSchema<RowData> createPscDeserializationSchema(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {
        final DynamicPscDeserializationSchema.MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(DynamicPscDeserializationSchema.MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                DataType.getFieldDataTypes(producedDataType).size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                                IntStream.of(valueOutputProjection),
                                IntStream.range(
                                        keyOutputProjection.length + valueOutputProjection.length,
                                        adjustedPhysicalArity))
                        .toArray();

        return new DynamicPscDeserializationSchema(
                adjustedPhysicalArity,
                keyDeserialization,
                keyOutputProjection,
                valueDeserialization,
                adjustedValueProjection,
                hasMetadata,
                metadataConverters,
                producedTypeInfo,
                upsertMode);
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            int[][] nestedProjection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        // Use nested projection if available for proper nested field pruning
        DataType physicalFormatDataType;
        if (nestedProjection != null && nestedProjection.length > 0) {
            physicalFormatDataType = Projection.of(nestedProjection).project(this.physicalDataType);
        } else {
            physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        }
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        TOPIC_URI(
                "topic-uri",
                DataTypes.STRING().notNull(),
                new DynamicPscDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(PscConsumerMessage<?, ?> record) {
                        return StringData.fromString(record.getMessageId().getTopicUriPartition().getTopicUriAsString());
                    }
                }),

        PARTITION(
                "partition",
                DataTypes.INT().notNull(),
                new DynamicPscDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(PscConsumerMessage<?, ?> record) {
                        return record.getMessageId().getTopicUriPartition().getPartition();
                    }
                }),

        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .notNull(),
                new DynamicPscDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(PscConsumerMessage<?, ?> record) {
                        final Map<StringData, byte[]> map = new HashMap<>();
                        for (Map.Entry<String, byte[]> header : record.getHeaders().entrySet()) {
                            if (!header.getKey().startsWith("psc."))
                                map.put(StringData.fromString(header.getKey()), header.getValue());                        }
                        return new GenericMapData(map);
                    }
                }),

        PSC_HEADERS(
                "psc-headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .notNull(),
                new DynamicPscDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(PscConsumerMessage<?, ?> record) {
                        final Map<StringData, byte[]> map = new HashMap<>();
                        for (Map.Entry<String, byte[]> header : record.getHeaders().entrySet()) {
                            if (header.getKey().startsWith("psc."))
                                map.put(StringData.fromString(header.getKey()), header.getValue());
                        }
                        return new GenericMapData(map);
                    }
                }),

        // leader epoch is not supported
        LEADER_EPOCH(
                "leader-epoch",
                DataTypes.INT().nullable(),
                new DynamicPscDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(PscConsumerMessage<?, ?> record) {
                        throw new UnsupportedOperationException("Leader epoch is not supported.");
                    }
                }),

        OFFSET(
                "offset",
                DataTypes.BIGINT().notNull(),
                new DynamicPscDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(PscConsumerMessage<?, ?> record) {
                        return record.getMessageId().getOffset();
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new DynamicPscDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(PscConsumerMessage<?, ?> record) {
                        return TimestampData.fromEpochMillis(record.getMessageId().getTimestamp());
                    }
                }),

        // timestamp_type is not supported
        TIMESTAMP_TYPE(
                "timestamp-type",
                DataTypes.STRING().notNull(),
                new DynamicPscDeserializationSchema.MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(PscConsumerMessage<?, ?> record) {
                        throw new UnsupportedOperationException("Timestamp type is not supported"); // TODO: figure out if this is needed
                    }
                });

        final String key;

        final DataType dataType;

        final DynamicPscDeserializationSchema.MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, DynamicPscDeserializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
