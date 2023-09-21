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

import com.pinterest.flink.streaming.connectors.psc.FlinkPscConsumerBase;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * A version-agnostic PSC {@link ScanTableSource}.
 *
 * <p>The version-specific PSC consumers need to extend this class and
 * override {@link #createPscConsumer(String, Properties, DeserializationSchema)}}.
 */
@Internal
public abstract class PscDynamicSourceBase implements ScanTableSource {

    // --------------------------------------------------------------------------------------------
    // Common attributes
    // --------------------------------------------------------------------------------------------
    protected final DataType outputDataType;

    // --------------------------------------------------------------------------------------------
    // Scan format attributes
    // --------------------------------------------------------------------------------------------

    /**
     * Scan format for decoding messages using PSC consumer.
     */
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    // --------------------------------------------------------------------------------------------
    // Kafka-specific attributes
    // --------------------------------------------------------------------------------------------

    /**
     * The PSC topicUri URI to consume.
     */
    protected final String topicUri;

    /**
     * Properties for the PSC consumer.
     */
    protected final Properties properties;

    /**
     * The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}).
     */
    protected final StartupMode startupMode;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}.
     */
    protected final Map<PscTopicUriPartition, Long> specificStartupOffsets;

    /**
     * The start timestamp to locate partition offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.
     */
    protected final long startupTimestampMillis;

    /**
     * The default value when startup timestamp is not used.
     */
    private static final long DEFAULT_STARTUP_TIMESTAMP_MILLIS = 0L;

    /**
     * Creates a generic Kafka {@link StreamTableSource}.
     *
     * @param outputDataType         Source produced data type
     * @param topicUri               Kafka topicUri to consume.
     * @param properties             Properties for the Kafka consumer.
     * @param decodingFormat         Decoding format for decoding records from Kafka.
     * @param startupMode            Startup mode for the contained consumer.
     * @param specificStartupOffsets Specific startup offsets; only relevant when startup
     *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}.
     * @param startupTimestampMillis Startup timestamp for offsets; only relevant when startup
     *                               mode is {@link StartupMode#TIMESTAMP}.
     */
    protected PscDynamicSourceBase(
            DataType outputDataType,
            String topicUri,
            Properties properties,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            StartupMode startupMode,
            Map<PscTopicUriPartition, Long> specificStartupOffsets,
            long startupTimestampMillis) {
        this.outputDataType = Preconditions.checkNotNull(
                outputDataType, "Produced data type must not be null.");
        this.topicUri = Preconditions.checkNotNull(topicUri, "Topic must not be null.");
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.decodingFormat = Preconditions.checkNotNull(
                decodingFormat, "Decoding format must not be null.");
        this.startupMode = Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
        this.specificStartupOffsets = Preconditions.checkNotNull(
                specificStartupOffsets, "Specific offsets must not be null.");
        this.startupTimestampMillis = startupTimestampMillis;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return this.decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        DeserializationSchema<RowData> deserializationSchema =
                this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.outputDataType);
        // Version-specific Kafka consumer
        FlinkPscConsumerBase<RowData> pscConsumer =
                getPscConsumer(topicUri, properties, deserializationSchema);
        return SourceFunctionProvider.of(pscConsumer, false);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PscDynamicSourceBase that = (PscDynamicSourceBase) o;
        return Objects.equals(outputDataType, that.outputDataType) &&
                Objects.equals(topicUri, that.topicUri) &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(decodingFormat, that.decodingFormat) &&
                startupMode == that.startupMode &&
                Objects.equals(specificStartupOffsets, that.specificStartupOffsets) &&
                startupTimestampMillis == that.startupTimestampMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                outputDataType,
                topicUri,
                properties,
                decodingFormat,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis);
    }

    // --------------------------------------------------------------------------------------------
    // Abstract methods for subclasses
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a version-specific Kafka consumer.
     *
     * @param topicUri              Kafka topicUri to consume.
     * @param properties            Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema to use for Kafka records.
     * @return The version-specific Kafka consumer
     */
    protected abstract FlinkPscConsumerBase<RowData> createPscConsumer(
            String topicUri,
            Properties properties,
            DeserializationSchema<RowData> deserializationSchema);

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Returns a version-specific Kafka consumer with the start position configured.
     *
     * @param topicUri              Kafka topicUri to consume.
     * @param properties            Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema to use for Kafka records.
     * @return The version-specific Kafka consumer
     */
    protected FlinkPscConsumerBase<RowData> getPscConsumer(
            String topicUri,
            Properties properties,
            DeserializationSchema<RowData> deserializationSchema) {
        FlinkPscConsumerBase<RowData> kafkaConsumer =
                createPscConsumer(topicUri, properties, deserializationSchema);
        switch (startupMode) {
            case EARLIEST:
                kafkaConsumer.setStartFromEarliest();
                break;
            case LATEST:
                kafkaConsumer.setStartFromLatest();
                break;
            case GROUP_OFFSETS:
                kafkaConsumer.setStartFromGroupOffsets();
                break;
            case SPECIFIC_OFFSETS:
                kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
                break;
            case TIMESTAMP:
                kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
                break;
        }
        kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID) != null);
        return kafkaConsumer;
    }
}
