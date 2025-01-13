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

package com.pinterest.flink.connector.psc.source;

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumState;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumStateSerializer;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumerator;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriber;
import com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics;
import com.pinterest.flink.connector.psc.source.reader.PscRecordEmitter;
import com.pinterest.flink.connector.psc.source.reader.PscSourceReader;
import com.pinterest.flink.connector.psc.source.reader.PscTopicUriPartitionSplitReader;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.source.reader.fetcher.PscSourceFetcherManager;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplitSerializer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.SerializableSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The Source implementation of PSC. Please use a {@link PscSourceBuilder} to construct a {@link
 * PscSource}
 *
 * This is different from {@link com.pinterest.flink.streaming.connectors.psc.FlinkPscConsumer}
 * in that it is a source implementation for Flink's new source API. Due to the difference in implementation,
 * the two classes are not compatible with each other. Most notably, PscSource currently can only support
 * reading from topics from a single backend cluster, while FlinkPscConsumer can read from multiple clusters.
 * This limitation is due to the fact that a {@link com.pinterest.flink.connector.psc.PscFlinkConfiguration#CLUSTER_URI_CONFIG}
 * is required to be set in the configuration for the PscSource to perform metadata queries against the cluster using
 * a {@link com.pinterest.psc.metadata.client.PscMetadataClient}.
 *
 * The following example shows how to create a PscSource emitting records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * PscSource<String> source = PscSource
 *     .<String>builder()
 *     .setGroupId("MyGroup")
 *     .setTopicUris(Arrays.asList(TOPIC1, TOPIC2))
 *     .setDeserializer(new TestingPscRecordDeserializationSchema())
 *     .setStartingOffsets(OffsetsInitializer.earliest())
 *     .build();
 * }</pre>
 *
 * <p>{@link org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator} only supports
 * adding new splits and not removing splits in split discovery.
 *
 * <p>See {@link PscSourceBuilder} for more details on how to configure this source.
 *
 * @param <OUT> the output type of the source.
 */
@PublicEvolving
public class PscSource<OUT>
        implements Source<OUT, PscTopicUriPartitionSplit, PscSourceEnumState>,
                ResultTypeQueryable<OUT> {
    private static final long serialVersionUID = -8755372893283732098L;
    // Users can choose only one of the following ways to specify the topics to consume from.
    private final PscSubscriber subscriber;
    // Users can specify the starting / stopping offset initializer.
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;
    // Boundedness
    private final Boundedness boundedness;
    private final PscRecordDeserializationSchema<OUT> deserializationSchema;
    // The configurations.
    private final Properties props;
    // Client rackId callback
    private final SerializableSupplier<String> rackIdSupplier;

    PscSource(
            PscSubscriber subscriber,
            OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            PscRecordDeserializationSchema<OUT> deserializationSchema,
            Properties props,
            SerializableSupplier<String> rackIdSupplier) {
        this.subscriber = subscriber;
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.boundedness = boundedness;
        this.deserializationSchema = deserializationSchema;
        this.props = props;
        this.rackIdSupplier = rackIdSupplier;
    }

    /**
     * Get a PscSourceBuilder to build a {@link PscSource}.
     *
     * @return a PSC source builder.
     */
    public static <OUT> PscSourceBuilder<OUT> builder() {
        return new PscSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Internal
    @Override
    public SourceReader<OUT, PscTopicUriPartitionSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return createReader(readerContext, (ignore) -> {});
    }

    @VisibleForTesting
    SourceReader<OUT, PscTopicUriPartitionSplit> createReader(
            SourceReaderContext readerContext, Consumer<Collection<String>> splitFinishedHook)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>>>
                elementsQueue = new FutureCompletingBlockingQueue<>();
        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });
        final PscSourceReaderMetrics pscSourceReaderMetrics =
                new PscSourceReaderMetrics(readerContext.metricGroup());

        Supplier<PscTopicUriPartitionSplitReader> splitReaderSupplier =
                () -> {
                    try {
                        return new PscTopicUriPartitionSplitReader(
                                props,
                                readerContext,
                                pscSourceReaderMetrics,
                                Optional.ofNullable(rackIdSupplier)
                                        .map(Supplier::get)
                                        .orElse(null));
                    } catch (ConfigurationException | ClientException e) {
                        throw new RuntimeException(e);
                    }
                };
        PscRecordEmitter<OUT> recordEmitter = new PscRecordEmitter<>(deserializationSchema);

        return new PscSourceReader<>(
                elementsQueue,
                new PscSourceFetcherManager(
                        elementsQueue, splitReaderSupplier::get, splitFinishedHook),
                recordEmitter,
                toConfiguration(props),
                readerContext,
                pscSourceReaderMetrics);
    }

    @Internal
    @Override
    public SplitEnumerator<PscTopicUriPartitionSplit, PscSourceEnumState> createEnumerator(
            SplitEnumeratorContext<PscTopicUriPartitionSplit> enumContext) {
        return new PscSourceEnumerator(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                enumContext,
                boundedness);
    }

    @Internal
    @Override
    public SplitEnumerator<PscTopicUriPartitionSplit, PscSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<PscTopicUriPartitionSplit> enumContext,
            PscSourceEnumState checkpoint)
            throws IOException {
        return new PscSourceEnumerator(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                enumContext,
                boundedness,
                checkpoint);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<PscTopicUriPartitionSplit> getSplitSerializer() {
        PscTopicUriPartitionSplitSerializer serializer = new PscTopicUriPartitionSplitSerializer();
        try {
            serializer.setClusterUri(PscFlinkConfiguration.validateAndGetBaseClusterUri(props).getTopicUriAsString());
            return serializer;
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException("Failed to set clusterUri for splitSerializer", e);
        }
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<PscSourceEnumState> getEnumeratorCheckpointSerializer() {
        PscSourceEnumStateSerializer serializer = new PscSourceEnumStateSerializer();
        try {
            serializer.setClusterUri(PscFlinkConfiguration.validateAndGetBaseClusterUri(props).getTopicUriAsString());
            return serializer;
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException("Failed to set clusterUri for enumStateSerializer", e);
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // ----------- private helper methods ---------------

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }

    @VisibleForTesting
    Configuration getConfiguration() {
        return toConfiguration(props);
    }

    @VisibleForTesting
    PscSubscriber getPscSubscriber() {
        return subscriber;
    }

    @VisibleForTesting
    OffsetsInitializer getStoppingOffsetsInitializer() {
        return stoppingOffsetsInitializer;
    }
}
