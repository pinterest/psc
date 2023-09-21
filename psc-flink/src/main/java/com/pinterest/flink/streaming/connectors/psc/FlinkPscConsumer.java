/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.streaming.connectors.psc.config.OffsetCommitMode;
import com.pinterest.flink.streaming.connectors.psc.internals.AbstractFetcher;
import com.pinterest.flink.streaming.connectors.psc.internals.AbstractTopicUriPartitionDiscoverer;
import com.pinterest.flink.streaming.connectors.psc.internals.PscConsumerThread;
import com.pinterest.flink.streaming.connectors.psc.internals.PscDeserializationSchemaWrapper;
import com.pinterest.flink.streaming.connectors.psc.internals.PscFetcher;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartitionDiscoverer;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUrisDescriptor;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getBoolean;
import static org.apache.flink.util.PropertiesUtil.getLong;

/**
 * The Flink PSC Consumer is a streaming data source that pulls a parallel
 * data stream using PSC. The consumer can run in multiple parallel
 * instances, each of which will pull data from one or more PSC topic URI partitions.
 *
 * <p>
 * The Flink PSC Consumer participates in checkpointing and guarantees that no
 * data is lost during a failure, and that the computation processes elements
 * "exactly once". (Note: These guarantees naturally assume that the backend pubsub
 * itself does not loose any data.)
 * </p>
 *
 * <p>
 * Please note that Flink snapshots the offsets internally as part of its
 * distributed checkpoints. The offsets committed are only to bring the
 * outside view of progress in sync with Flink's view of the progress. That way,
 * monitoring and other jobs can get a view of how far the Flink PSC consumer
 * has consumed a topic.
 * </p>
 *
 * <p>
 * Please refer to PSC consumer configuration.
 * </p>
 */
@PublicEvolving
public class FlinkPscConsumer<T> extends FlinkPscConsumerBase<T> {

    /**
     * Configuration key to change the polling timeout.
     **/
    public static final String KEY_POLL_TIMEOUT = "flink.poll-timeout";

    /**
     * The time, in milliseconds, spent waiting in poll if
     * data is not available. If 0, returns immediately with any records that are
     * available now.
     */
    public static final long DEFAULT_POLL_TIMEOUT = 100L;

    // ------------------------------------------------------------------------

    /**
     * The time, in milliseconds, spent waiting in poll if
     * data is not available. If 0, returns immediately with any records that are
     * available now
     */
    protected final long pollTimeout;

    private transient AtomicBoolean pscMetricsInitialized;

    // ------------------------------------------------------------------------

    /**
     * Creates a new PSC streaming source consumer.
     *
     * @param topicUri          The topic URI that should be consumed.
     * @param valueDeserializer The de-/serializer used to convert between PSC's
     *                          byte messages and Flink's objects.
     * @param props
     */
    public FlinkPscConsumer(String topicUri,
                            DeserializationSchema<T> valueDeserializer,
                            Properties props) {
        this(Collections.singletonList(topicUri), valueDeserializer, props);
    }

    /**
     * Creates a new PSC streaming source consumer.
     *
     * <p>
     * This constructor allows passing a {@see PscDeserializationSchema} for
     * reading key/value pairs, offsets, and topic URIs.
     *
     * @param topicUri     The topic URI that should be consumed.
     * @param deserializer The keyed de-/serializer used to convert between PSC's
     *                     byte messages and Flink's objects.
     * @param props
     */
    public FlinkPscConsumer(String topicUri,
                            PscDeserializationSchema<T> deserializer,
                            Properties props) {
        this(Collections.singletonList(topicUri), deserializer, props);
    }

    /**
     * Creates a new PSC streaming source consumer.
     *
     * <p>
     * This constructor allows passing multiple topic URIs to the consumer.
     *
     * @param topicUris    The PSC topic URIs to read from.
     * @param deserializer The de-/serializer used to convert between PSC's byte
     *                     messages and Flink's objects.
     * @param props
     */
    public FlinkPscConsumer(List<String> topicUris,
                            DeserializationSchema<T> deserializer,
                            Properties props) {
        this(topicUris, new PscDeserializationSchemaWrapper<>(deserializer), props);
    }

    /**
     * Creates a new PSC streaming source consumer.
     *
     * <p>
     * This constructor allows passing multiple topics and a key/value
     * deserialization schema.
     *
     * @param topicUris    The PSC topic URIs to read from.
     * @param deserializer The keyed de-/serializer used to convert between PSC's
     *                     byte messages and Flink's objects.
     * @param props
     */
    public FlinkPscConsumer(List<String> topicUris,
                            PscDeserializationSchema<T> deserializer,
                            Properties props) {
        this(topicUris, null, deserializer, props);
    }

    /**
     * Creates a new PSC streaming source consumer. Use this constructor to
     * subscribe to multiple topics based on a regular expression pattern.
     *
     * <p>
     * If partition discovery is enabled (by setting a non-negative value for
     * {@link FlinkPscConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the
     * properties), topics with names matching the pattern will also be subscribed
     * to as they are created on the fly.
     *
     * @param subscriptionPattern The regular expression for a pattern of topic
     *                            names to subscribe to.
     * @param valueDeserializer   The de-/serializer used to convert between PSC's
     *                            byte messages and Flink's objects.
     * @param props
     */
    public FlinkPscConsumer(Pattern subscriptionPattern,
                            DeserializationSchema<T> valueDeserializer,
                            Properties props) {
        this(null, subscriptionPattern, new PscDeserializationSchemaWrapper<>(valueDeserializer),
                props);
    }

    /**
     * Creates a new PSC streaming source consumer. Use this constructor to
     * subscribe to multiple topics based on a regular expression pattern.
     *
     * <p>
     * If partition discovery is enabled (by setting a non-negative value for
     * {@link FlinkPscConsumer#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the
     * properties), topics with names matching the pattern will also be subscribed
     * to as they are created on the fly.
     *
     * <p>
     * This constructor allows passing a {@see PscDeserializationSchema} for
     * reading key/value pairs, offsets, and topic URIs.
     *
     * @param subscriptionPattern The regular expression for a pattern of topic
     *                            names to subscribe to.
     * @param deserializer        The keyed de-/serializer used to convert between
     *                            PSC's byte messages and Flink's objects.
     * @param props
     */
    public FlinkPscConsumer(Pattern subscriptionPattern,
                            PscDeserializationSchema<T> deserializer,
                            Properties props) {
        this(null, subscriptionPattern, deserializer, props);
    }

    private FlinkPscConsumer(List<String> topicUris,
                             Pattern subscriptionPattern,
                             PscDeserializationSchema<T> deserializer,
                             Properties props) {

        super(
                topicUris, subscriptionPattern, deserializer, getLong(checkNotNull(props, "props"),
                        KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, PARTITION_DISCOVERY_DISABLED),
                !getBoolean(props, KEY_DISABLE_METRICS, false), props);
        setDeserializer(this.properties);

        // configure the polling timeout
        try {
            if (properties.containsKey(KEY_POLL_TIMEOUT)) {
                this.pollTimeout = Long.parseLong(properties.getProperty(KEY_POLL_TIMEOUT));
            } else {
                this.pollTimeout = DEFAULT_POLL_TIMEOUT;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Cannot parse poll timeout for '" + KEY_POLL_TIMEOUT + '\'', e);
        }

        initializePscMetrics();
    }

    @Override
    protected AbstractFetcher<T, ?> createFetcher(SourceContext<T> sourceContext,
                                                  Map<PscTopicUriPartition, Long> assignedPscTopicUriPartitionsWithInitialOffsets,
                                                  SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
                                                  StreamingRuntimeContext runtimeContext,
                                                  OffsetCommitMode offsetCommitMode,
                                                  MetricGroup pubsubMetricGroup,
                                                  boolean useMetrics) throws Exception {

        // make sure that auto commit is disabled when our offset commit mode is
        // ON_CHECKPOINTS;
        // this overwrites whatever setting the user configured in the properties
        adjustAutoCommitConfig(properties, offsetCommitMode);

        return new PscFetcher<>(sourceContext, assignedPscTopicUriPartitionsWithInitialOffsets,
                                watermarkStrategy, runtimeContext.getProcessingTimeService(),
                                runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                                runtimeContext.getUserCodeClassLoader(), runtimeContext.getTaskNameWithSubtasks(),
                                deserializer, properties, pollTimeout, runtimeContext.getMetricGroup(), pubsubMetricGroup,
                                useMetrics);
    }

    @Override
    protected AbstractTopicUriPartitionDiscoverer createTopicUriPartitionDiscoverer(
            PscTopicUrisDescriptor topicUrisDescriptor,
            int indexOfThisSubtask,
            int numParallelSubtasks) {

        return new PscTopicUriPartitionDiscoverer(topicUrisDescriptor, indexOfThisSubtask, numParallelSubtasks,
                                                  properties);
    }

    @Override
    protected Map<PscTopicUriPartition, Long> fetchOffsetsWithTimestamp(Collection<PscTopicUriPartition> partitions,
                                                                        long timestamp) {

        Map<TopicUriPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());
        for (PscTopicUriPartition partition : partitions) {
            partitionOffsetsRequest
                    .put(new TopicUriPartition(partition.getTopicUriStr(), partition.getPartition()), timestamp);
        }

        final Map<PscTopicUriPartition, Long> result = new HashMap<>(partitions.size());
        // use a short-lived consumer to fetch the offsets;
        // this is ok because this is a one-time operation that happens only on startup
        PscConsumer<?, ?> pscConsumer = null;
        try {
            pscConsumer = PscConsumerThread.getConsumer(properties);
            for (Map.Entry<TopicUriPartition, MessageId> partitionToOffset : pscConsumer
                    .getMessageIdByTimestamp(partitionOffsetsRequest).entrySet()) {
                result.put(
                        new PscTopicUriPartition(partitionToOffset.getKey().getTopicUriAsString(),
                                partitionToOffset.getKey().getPartition()),
                        (partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().getOffset());
            }
        } catch (ConfigurationException | ConsumerException e) {
            throw new RuntimeException(e);
        } finally {
            if (pscConsumer != null) {
                try {
                    pscConsumer.close();
                } catch (ConsumerException e) {
                    // failed to close consumer
                    LOG.error("Failed to close consumer", e);
                }
            }
        }
        return result;
    }

    @Override
    protected boolean getIsAutoCommitEnabled() {
        return getBoolean(properties, PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, true) && PropertiesUtil
                .getLong(properties, "psc.consumer.auto.commit.interval.ms", 5000) > 0;
    }

    @Override
    public void close() throws Exception {
        if (pscMetricsInitialized != null && pscMetricsInitialized.compareAndSet(true, false)) {
            PscMetricRegistryManager.getInstance().shutdown(pscConfigurationInternal);
        }
        super.close();
    }

    /**
     * Makes sure that the ByteArrayDeserializer is registered in the PSC
     * properties.
     *
     * @param props The PSC properties to register the serializer in.
     */
    private static void setDeserializer(Properties props) {
        final String deSerName = ByteArrayDeserializer.class.getName();

        Object keyDeSer = props.get(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER);
        Object valDeSer = props.get(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER);

        if (keyDeSer != null && !keyDeSer.equals(deSerName)) {
            LOG.warn("Ignoring configured key DeSerializer ({})",
                    PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER);
        }
        if (valDeSer != null && !valDeSer.equals(deSerName)) {
            LOG.warn("Ignoring configured value DeSerializer ({})",
                    PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER);
        }

        props.put(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, deSerName);
        props.put(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, deSerName);
    }

    @Override
    protected void initializePscMetrics() {
        super.initializePscMetrics();

        if (pscMetricsInitialized == null)
            pscMetricsInitialized = new AtomicBoolean(false);

        if (pscMetricsInitialized.compareAndSet(false, true)) {
            PscMetricRegistryManager.getInstance().initialize(pscConfigurationInternal);
        }
    }
}
