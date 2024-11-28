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
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.subscriber.PscStreamSetSubscriber;
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.subscriber.PscStreamSubscriber;
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.subscriber.StreamPatternSubscriber;
import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/** A builder class to make it easier for users to construct a {@link DynamicPscSource}. */
@Experimental
public class DynamicPscSourceBuilder<T> {
    private static final Logger logger = LoggerFactory.getLogger(DynamicPscSourceBuilder.class);
    private PscStreamSubscriber pscStreamSubscriber;
    private PscMetadataService pscMetadataService;
    private PscRecordDeserializationSchema<T> deserializationSchema;
    private OffsetsInitializer startingOffsetsInitializer;
    private OffsetsInitializer stoppingOffsetsInitializer;
    private Boundedness boundedness;
    private final Properties props;

    DynamicPscSourceBuilder() {
        this.pscStreamSubscriber = null;
        this.pscMetadataService = null;
        this.deserializationSchema = null;
        this.startingOffsetsInitializer = OffsetsInitializer.earliest();
        this.stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.props = new Properties();
    }

    /**
     * Set the stream ids belonging to the {@link PscMetadataService}.
     *
     * @param streamIds the stream ids.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setStreamIds(Set<String> streamIds) {
        Preconditions.checkNotNull(streamIds);
        ensureSubscriberIsNull("streamIds");
        this.pscStreamSubscriber = new PscStreamSetSubscriber(streamIds);
        return this;
    }

    /**
     * Set the stream pattern to determine stream ids belonging to the {@link PscMetadataService}.
     *
     * @param streamPattern the stream pattern.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setStreamPattern(Pattern streamPattern) {
        Preconditions.checkNotNull(streamPattern);
        ensureSubscriberIsNull("stream pattern");
        this.pscStreamSubscriber = new StreamPatternSubscriber(streamPattern);
        return this;
    }

    /**
     * Set a custom Psc stream subscriber.
     *
     * @param pscStreamSubscriber the {@link PscStreamSubscriber}.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setPscStreamSubscriber(
            PscStreamSubscriber pscStreamSubscriber) {
        Preconditions.checkNotNull(pscStreamSubscriber);
        ensureSubscriberIsNull("custom");
        this.pscStreamSubscriber = pscStreamSubscriber;
        return this;
    }

    /**
     * Set the source in bounded mode and specify what offsets to end at. This is used for all
     * clusters.
     *
     * @param stoppingOffsetsInitializer the {@link OffsetsInitializer}.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setBounded(OffsetsInitializer stoppingOffsetsInitializer) {
        this.boundedness = Boundedness.BOUNDED;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        return this;
    }

    /**
     * Set the {@link PscMetadataService}.
     *
     * @param pscMetadataService the {@link PscMetadataService}.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setPscMetadataService(
            PscMetadataService pscMetadataService) {
        this.pscMetadataService = pscMetadataService;
        return this;
    }

    /**
     * Set the {@link PscRecordDeserializationSchema}.
     *
     * @param recordDeserializer the {@link PscRecordDeserializationSchema}.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setDeserializer(
            PscRecordDeserializationSchema<T> recordDeserializer) {
        this.deserializationSchema = recordDeserializer;
        return this;
    }

    /**
     * Set the starting offsets of the stream. This will be applied to all clusters.
     *
     * @param startingOffsetsInitializer the {@link OffsetsInitializer}.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setStartingOffsets(
            OffsetsInitializer startingOffsetsInitializer) {
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        return this;
    }

    /**
     * Set the properties of the consumers. This will be applied to all clusters and properties like
     * {@link com.pinterest.flink.connector.psc.PscFlinkConfiguration#CLUSTER_URI_CONFIG} may be overriden by the {@link
     * PscMetadataService}.
     *
     * @param properties the properties.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setProperties(Properties properties) {
        this.props.putAll(properties);
        return this;
    }

    /**
     * Set a property for the consumers. This will be applied to all clusters and properties like
     * {@link com.pinterest.flink.connector.psc.PscFlinkConfiguration#CLUSTER_URI_CONFIG} may be overriden by the {@link
     * PscMetadataService}.
     *
     * @param key the property key.
     * @param value the properties value.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setProperty(String key, String value) {
        this.props.setProperty(key, value);
        return this;
    }

//    /**
//     * Set the property for {@link CommonClientConfigs#GROUP_ID_CONFIG}. This will be applied to all
//     * clusters.
//     *
//     * @param groupId the group id.
//     * @return the builder.
//     */
//    public DynamicPscSourceBuilder<T> setGroupId(String groupId) {
//        return setProperty(CommonClientConfigs.GROUP_ID_CONFIG, groupId);
//    }

    /**
     * Set the client id prefix. This applies {@link PscSourceOptions#CLIENT_ID_PREFIX} to all
     * clusters.
     *
     * @param prefix the client id prefix.
     * @return the builder.
     */
    public DynamicPscSourceBuilder<T> setClientIdPrefix(String prefix) {
        return setProperty(PscSourceOptions.CLIENT_ID_PREFIX.key(), prefix);
    }

    /**
     * Construct the source with the configuration that was set.
     *
     * @return the {@link DynamicPscSource}.
     */
    public DynamicPscSource<T> build() {
        logger.info("Building the DynamicPscSource");
        sanityCheck();
        setRequiredConsumerProperties();
        return new DynamicPscSource<>(
                pscStreamSubscriber,
                pscMetadataService,
                deserializationSchema,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                boundedness);
    }

    // Below are utility methods, code and structure are mostly copied over from PscSourceBuilder

    private void setRequiredConsumerProperties() {
        maybeOverride(
                PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER,
                ByteArrayDeserializer.class.getName(),
                true);
        maybeOverride(
                PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER,
                ByteArrayDeserializer.class.getName(),
                true);
        if (!props.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID)) {
            logger.warn(
                    "Offset commit on checkpoint is disabled because {} is not specified",
                    PscConfiguration.PSC_CONSUMER_GROUP_ID);
            maybeOverride(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false", false);
        }
        maybeOverride(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false", false);
        maybeOverride(
                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET,
                startingOffsetsInitializer.getAutoOffsetResetStrategy().toLowerCase(),
                true);

        // If the source is bounded, do not run periodic partition discovery.
        maybeOverride(
                PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                "-1",
                boundedness == Boundedness.BOUNDED);

        // If the source is bounded, do not run periodic metadata discovery
        maybeOverride(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(),
                "-1",
                boundedness == Boundedness.BOUNDED);
        maybeOverride(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                "0",
                boundedness == Boundedness.BOUNDED);

        // If the client id prefix is not set, reuse the consumer group id as the client id prefix,
        // or generate a random string if consumer group id is not specified.
        maybeOverride(
                PscSourceOptions.CLIENT_ID_PREFIX.key(),
                props.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID)
                        ? props.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID)
                        : "DynamicPscSource-" + RandomStringUtils.randomAlphabetic(8),
                false);
    }

    private boolean maybeOverride(String key, String value, boolean override) {
        boolean overridden = false;
        String userValue = props.getProperty(key);
        if (userValue != null) {
            if (override) {
                logger.warn(
                        String.format(
                                "Property %s is provided but will be overridden from %s to %s",
                                key, userValue, value));
                props.setProperty(key, value);
                overridden = true;
            }
        } else {
            props.setProperty(key, value);
        }
        return overridden;
    }

    private void sanityCheck() {
        Preconditions.checkNotNull(
                pscStreamSubscriber, "Psc stream subscriber is required but not provided");
        Preconditions.checkNotNull(
                pscMetadataService, "Psc Metadata Service is required but not provided");
        Preconditions.checkNotNull(
                deserializationSchema, "Deserialization schema is required but not provided.");

        // Check consumer group ID
        Preconditions.checkState(
                props.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID) || !offsetCommitEnabledManually(),
                String.format(
                        "Property %s is required when offset commit is enabled",
                        PscConfiguration.PSC_CONSUMER_GROUP_ID));
    }

    private boolean offsetCommitEnabledManually() {
        boolean autoCommit =
                props.containsKey(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED)
                        && Boolean.parseBoolean(
                                props.getProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED));
        boolean commitOnCheckpoint =
                props.containsKey(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key())
                        && Boolean.parseBoolean(
                                props.getProperty(
                                        PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key()));
        return autoCommit || commitOnCheckpoint;
    }

    private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
        if (pscStreamSubscriber != null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot use %s for consumption because a %s is already set for consumption.",
                            attemptingSubscribeMode,
                            pscStreamSubscriber.getClass().getSimpleName()));
        }
    }
}
