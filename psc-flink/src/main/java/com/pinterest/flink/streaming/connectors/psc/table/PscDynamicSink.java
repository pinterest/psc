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

import com.pinterest.flink.connector.psc.sink.PscSink;
import com.pinterest.flink.connector.psc.sink.PscSinkBuilder;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A version-agnostic PSC {@link DynamicTableSink}. */
@Internal
public class PscDynamicSink implements DynamicTableSink, SupportsWritingMetadata {

    private static final String UPSERT_PSC_TRANSFORMATION = "upsert-psc";

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Metadata that is appended at the end of a physical sink row. */
    protected List<String> metadataKeys;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    /** Data type of consumed data type. */
    protected DataType consumedDataType;

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /** Optional format for encoding keys to PSC. */
    protected final @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;

    /** Format for encoding values to PSC. */
    protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    /** Indices that determine the key fields and the source position in the consumed row. */
    protected final int[] keyProjection;

    /** Indices that determine the value fields and the source position in the consumed row. */
    protected final int[] valueProjection;

    /** Prefix that needs to be removed from fields when constructing the physical data type. */
    protected final @Nullable String keyPrefix;

    // --------------------------------------------------------------------------------------------
    // PSC-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The defined delivery guarantee. */
    private final DeliveryGuarantee deliveryGuarantee;

    /**
     * If the {@link #deliveryGuarantee} is {@link DeliveryGuarantee#EXACTLY_ONCE} the value is the
     * prefix for all ids of opened PubSub transactions.
     */
    @Nullable private final String transactionalIdPrefix;

    /** The PubSub topic to write to. */
    protected final String topic;

    /** Properties for the PSC producer. */
    protected final Properties properties;

    /** Partitioner to select PubSub partition for each item. */
    protected final @Nullable FlinkPscPartitioner<RowData> partitioner;

    /**
     * Flag to determine sink mode. In upsert mode sink transforms the delete/update-before message
     * to tombstone message.
     */
    protected final boolean upsertMode;

    /** Sink buffer flush config which only supported in upsert mode now. */
    protected final SinkBufferFlushMode flushMode;

    /** Parallelism of the physical PubSub producer. * */
    protected final @Nullable Integer parallelism;

    public PscDynamicSink(
            DataType consumedDataType,
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
            boolean upsertMode,
            SinkBufferFlushMode flushMode,
            @Nullable Integer parallelism,
            @Nullable String transactionalIdPrefix) {
        // Format attributes
        this.consumedDataType =
                checkNotNull(consumedDataType, "Consumed data type must not be null.");
        this.physicalDataType =
                checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.keyEncodingFormat = keyEncodingFormat;
        this.valueEncodingFormat =
                checkNotNull(valueEncodingFormat, "Value encoding format must not be null.");
        this.keyProjection = checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection = checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;
        this.transactionalIdPrefix = transactionalIdPrefix;
        // Mutable attributes
        this.metadataKeys = Collections.emptyList();
        // PSC-specific attributes
        this.topic = checkNotNull(topic, "Topic must not be null.");
        this.properties = checkNotNull(properties, "Properties must not be null.");
        this.partitioner = partitioner;
        this.deliveryGuarantee =
                checkNotNull(deliveryGuarantee, "DeliveryGuarantee must not be null.");
        this.upsertMode = upsertMode;
        this.flushMode = checkNotNull(flushMode);
        if (flushMode.isEnabled() && !upsertMode) {
            throw new IllegalArgumentException(
                    "Sink buffer flush is only supported in upsert-psc.");
        }
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> keySerialization =
                createSerialization(context, keyEncodingFormat, keyProjection, keyPrefix);

        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection, null);

        final PscSinkBuilder<RowData> sinkBuilder = PscSink.builder();
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();
        if (transactionalIdPrefix != null) {
            sinkBuilder.setTransactionalIdPrefix(transactionalIdPrefix);
        }
        final PscSink<RowData> pscSink =
                sinkBuilder
                        .setDeliveryGuarantee(deliveryGuarantee)
                        .setPscProducerConfig(properties)
                        .setRecordSerializer(
                                new DynamicPscRecordSerializationSchema(
                                        topic,
                                        partitioner,
                                        keySerialization,
                                        valueSerialization,
                                        getFieldGetters(physicalChildren, keyProjection),
                                        getFieldGetters(physicalChildren, valueProjection),
                                        hasMetadata(),
                                        getMetadataPositions(physicalChildren),
                                        upsertMode))
                        .build();
        if (flushMode.isEnabled() && upsertMode) {
            return new DataStreamSinkProvider() {
                @Override
                public DataStreamSink<?> consumeDataStream(
                        ProviderContext providerContext, DataStream<RowData> dataStream) {
                    final boolean objectReuse =
                            dataStream.getExecutionEnvironment().getConfig().isObjectReuseEnabled();
                    final ReducingUpsertSink<?, ?> sink =
                            new ReducingUpsertSink<>(
                                    pscSink,
                                    physicalDataType,
                                    keyProjection,
                                    flushMode,
                                    objectReuse ? createRowDataTypeSerializer(context, dataStream.getExecutionConfig())::copy : rowData -> rowData);
                    final DataStreamSink<RowData> end = dataStream.sinkTo(sink);
                    providerContext.generateUid(UPSERT_PSC_TRANSFORMATION).ifPresent(end::uid);
                    if (parallelism != null) {
                        end.setParallelism(parallelism);
                    }
                    return end;
                }
            };
        }
        return SinkV2Provider.of(pscSink, parallelism);
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(WritableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
        this.consumedDataType = consumedDataType;
    }

    @Override
    public DynamicTableSink copy() {
        final PscDynamicSink copy =
                new PscDynamicSink(
                        consumedDataType,
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
                        upsertMode,
                        flushMode,
                        parallelism,
                        transactionalIdPrefix);
        copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "PSC table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PscDynamicSink that = (PscDynamicSink) o;
        return Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyEncodingFormat, that.keyEncodingFormat)
                && Objects.equals(valueEncodingFormat, that.valueEncodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(topic, that.topic)
                && Objects.equals(properties, that.properties)
                && Objects.equals(partitioner, that.partitioner)
                && Objects.equals(deliveryGuarantee, that.deliveryGuarantee)
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(flushMode, that.flushMode)
                && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                metadataKeys,
                consumedDataType,
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
                upsertMode,
                flushMode,
                transactionalIdPrefix,
                parallelism);
    }

    // --------------------------------------------------------------------------------------------

    private TypeSerializer<RowData> createRowDataTypeSerializer(
            Context context, ExecutionConfig executionConfig) {
        final TypeInformation<RowData> typeInformation =
                context.createTypeInformation(consumedDataType);
        return typeInformation.createSerializer(executionConfig);
    }

    private int[] getMetadataPositions(List<LogicalType> physicalChildren) {
        return Stream.of(WritableMetadata.values())
                .mapToInt(
                        m -> {
                            final int pos = metadataKeys.indexOf(m.key);
                            if (pos < 0) {
                                return -1;
                            }
                            return physicalChildren.size() + pos;
                        })
                .toArray();
    }

    private boolean hasMetadata() {
        return metadataKeys.size() > 0;
    }

    private RowData.FieldGetter[] getFieldGetters(
            List<LogicalType> physicalChildren, int[] keyProjection) {
        return Arrays.stream(keyProjection)
                .mapToObj(
                        targetField ->
                                RowData.createFieldGetter(
                                        physicalChildren.get(targetField), targetField))
                .toArray(RowData.FieldGetter[]::new);
    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum WritableMetadata {
        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        final MapData map = row.getMap(pos);
                        final ArrayData keyArray = map.keyArray();
                        final ArrayData valueArray = map.valueArray();
                        final Map<String, byte[]> headers = new HashMap<>();
                        for (int i = 0; i < keyArray.size(); i++) {
                            if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                                final String key = keyArray.getString(i).toString();
                                final byte[] value = valueArray.getBinary(i);
                                headers.put(key, value);
                            }
                        }
                        return headers;
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getTimestamp(pos, 3).getMillisecond();
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }
}
