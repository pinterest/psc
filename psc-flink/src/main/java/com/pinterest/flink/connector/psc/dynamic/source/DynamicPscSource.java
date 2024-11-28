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

package com.pinterest.flink.connector.psc.dynamic.source;

import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.DynamicPscSourceEnumState;
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.DynamicPscSourceEnumStateSerializer;
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.DynamicPscSourceEnumerator;
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.subscriber.PscStreamSubscriber;
import com.pinterest.flink.connector.psc.dynamic.source.reader.DynamicPscSourceReader;
import com.pinterest.flink.connector.psc.dynamic.source.split.DynamicPscSourceSplit;
import com.pinterest.flink.connector.psc.dynamic.source.split.DynamicPscSourceSplitSerializer;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Properties;

/**
 * Factory class for the DynamicPscSource components. <a
 * href="https://cwiki.apache.org/confluence/x/CBn1D">FLIP-246: DynamicPscSource</a>
 *
 * <p>This source's key difference from {@link com.pinterest.flink.connector.psc.source.PscSource} is that it enables users to read
 * dynamically, which does not require job restart, from streams (topics that belong to one or more
 * clusters). If using {@link com.pinterest.flink.connector.psc.source.PscSource}, users need to restart the job by deleting the job and
 * reconfiguring the topics and clusters.
 *
 * <p>This example shows how to configure a {@link DynamicPscSource} that emits Integer records:
 *
 * <pre>{@code
 * DynamicPscSource<Integer> dynamicPscSource =
 *                     DynamicPscSource.<Integer>builder()
 *                             .setStreamIds(Collections.singleton("MY_STREAM_ID"))
 *                             // custom metadata service that resolves `MY_STREAM_ID` to the associated clusters and topics
 *                             .setPscMetadataService(kafkaMetadataService)
 *                             .setDeserializer(
 *                                     PscRecordDeserializationSchema.valueOnly(
 *                                             IntegerDeserializer.class))
 *                             .setStartingOffsets(OffsetsInitializer.earliest())
 *                             // common properties for all Psc clusters
 *                             .setProperties(properties)
 *                             .build();
 * }</pre>
 *
 * <p>See more configuration options in {@link DynamicPscSourceBuilder} and {@link
 * DynamicPscSourceOptions}.
 *
 * @param <T> Record type
 */
@Experimental
public class DynamicPscSource<T>
        implements Source<T, DynamicPscSourceSplit, DynamicPscSourceEnumState>,
                ResultTypeQueryable<T> {

    private final PscStreamSubscriber pscStreamSubscriber;
    private final PscMetadataService pscMetadataService;
    private final PscRecordDeserializationSchema<T> deserializationSchema;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;
    private final Properties properties;
    private final Boundedness boundedness;

    DynamicPscSource(
            PscStreamSubscriber pscStreamSubscriber,
            PscMetadataService pscMetadataService,
            PscRecordDeserializationSchema<T> deserializationSchema,
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetsInitializer,
            Properties properties,
            Boundedness boundedness) {
        this.pscStreamSubscriber = pscStreamSubscriber;
        this.deserializationSchema = deserializationSchema;
        this.properties = properties;
        this.pscMetadataService = pscMetadataService;
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.boundedness = boundedness;
    }

    /**
     * Get a builder for this source.
     *
     * @return a {@link DynamicPscSourceBuilder}.
     */
    public static <T> DynamicPscSourceBuilder<T> builder() {
        return new DynamicPscSourceBuilder<>();
    }

    /**
     * Get the {@link Boundedness}.
     *
     * @return the {@link Boundedness}.
     */
    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    /**
     * Create the {@link DynamicPscSourceReader}.
     *
     * @param readerContext The {@link SourceReaderContext context} for the source reader.
     * @return the {@link DynamicPscSourceReader}.
     */
    @Internal
    @Override
    public SourceReader<T, DynamicPscSourceSplit> createReader(
            SourceReaderContext readerContext) {
        return new DynamicPscSourceReader<>(readerContext, deserializationSchema, properties);
    }

    /**
     * Create the {@link DynamicPscSourceEnumerator}.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the split enumerator.
     * @return the {@link DynamicPscSourceEnumerator}.
     */
    @Internal
    @Override
    public SplitEnumerator<DynamicPscSourceSplit, DynamicPscSourceEnumState> createEnumerator(
            SplitEnumeratorContext<DynamicPscSourceSplit> enumContext) {
        return new DynamicPscSourceEnumerator(
                pscStreamSubscriber,
                pscMetadataService,
                enumContext,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                properties,
                boundedness,
                new DynamicPscSourceEnumState());
    }

    /**
     * Restore the {@link DynamicPscSourceEnumerator}.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the restored split
     *     enumerator.
     * @param checkpoint The checkpoint to restore the SplitEnumerator from.
     * @return the {@link DynamicPscSourceEnumerator}.
     */
    @Internal
    @Override
    public SplitEnumerator<DynamicPscSourceSplit, DynamicPscSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<DynamicPscSourceSplit> enumContext,
            DynamicPscSourceEnumState checkpoint) {
        return new DynamicPscSourceEnumerator(
                pscStreamSubscriber,
                pscMetadataService,
                enumContext,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                properties,
                boundedness,
                checkpoint);
    }

    /**
     * Get the {@link DynamicPscSourceSplitSerializer}.
     *
     * @return the {@link DynamicPscSourceSplitSerializer}.
     */
    @Internal
    @Override
    public SimpleVersionedSerializer<DynamicPscSourceSplit> getSplitSerializer() {
        return new DynamicPscSourceSplitSerializer();
    }

    /**
     * Get the {@link DynamicPscSourceEnumStateSerializer}.
     *
     * @return the {@link DynamicPscSourceEnumStateSerializer}.
     */
    @Internal
    @Override
    public SimpleVersionedSerializer<DynamicPscSourceEnumState>
            getEnumeratorCheckpointSerializer() {
        return new DynamicPscSourceEnumStateSerializer();
    }

    /**
     * Get the {@link TypeInformation} of the source.
     *
     * @return the {@link TypeInformation}.
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @VisibleForTesting
    public PscStreamSubscriber getPscStreamSubscriber() {
        return pscStreamSubscriber;
    }
}
