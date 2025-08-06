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
import com.pinterest.flink.connector.psc.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializerValidator;
import com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriber;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.util.function.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The builder class for {@link PscSource} to make it easier for the users to construct a {@link
 * PscSource}.
 *
 * <p>The following example shows the minimum setup to create a PscSource that reads the String
 * values from a PSC topic.
 *
 * <pre>{@code
 * PscSource<String> source = PscSource
 *     .<String>builder()
 *     .setBootstrapServers(MY_BOOTSTRAP_SERVERS)
 *     .setTopics(Arrays.asList(TOPIC1, TOPIC2))
 *     .setDeserializer(PscRecordDeserializationSchema.valueOnly(StringDeserializer.class))
 *     .build();
 * }</pre>
 *
 * <p>The bootstrap servers, topics/partitions to consume, and the record deserializer are required
 * fields that must be set.
 *
 * <p>To specify the starting offsets of the PscSource, one can call {@link
 * #setStartingOffsets(OffsetsInitializer)}.
 *
 * <p>By default the PscSource runs in an {@link Boundedness#CONTINUOUS_UNBOUNDED} mode and never
 * stops until the Flink job is canceled or fails. To let the PscSource run in {@link
 * Boundedness#CONTINUOUS_UNBOUNDED} yet stop at some given offsets, one can call {@link
 * #setUnbounded(OffsetsInitializer)}. For example the following PscSource stops after it consumes
 * up to the latest partition offsets at the point when the Flink job started.
 *
 * <pre>{@code
 * PscSource<String> source = PscSource
 *     .<String>builder()
 *     .setBootstrapServers(MY_BOOTSTRAP_SERVERS)
 *     .setTopics(Arrays.asList(TOPIC1, TOPIC2))
 *     .setDeserializer(PscRecordDeserializationSchema.valueOnly(StringDeserializer.class))
 *     .setUnbounded(OffsetsInitializer.latest())
 *     .setRackId(() -> MY_RACK_ID)
 *     .build();
 * }</pre>
 *
 * <p>Check the Java docs of each individual methods to learn more about the settings to build a
 * PscSource.
 */
@PublicEvolving
public class PscSourceBuilder<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(PscSourceBuilder.class);
    private static final String[] REQUIRED_CONFIGS = {
            PscFlinkConfiguration.CLUSTER_URI_CONFIG,
    };
    // The subscriber specifies the partitions to subscribe to.
    private PscSubscriber subscriber;
    // Users can specify the starting / stopping offset initializer.
    private OffsetsInitializer startingOffsetsInitializer;
    private OffsetsInitializer stoppingOffsetsInitializer;
    // Boundedness
    private Boundedness boundedness;
    private PscRecordDeserializationSchema<OUT> deserializationSchema;
    // The configurations.
    protected Properties props;
    // Client rackId supplier
    private SerializableSupplier<String> rackIdSupplier;

    PscSourceBuilder() {
        this.subscriber = null;
        this.startingOffsetsInitializer = OffsetsInitializer.earliest();
        this.stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.deserializationSchema = null;
        this.props = new Properties();
        this.rackIdSupplier = null;
    }

    /**
     * Sets the clusterUri for the PscConsumer of the PscSource.
     *
     * @param clusterUri the clusterUri of the PubSub cluster.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setClusterUri(String clusterUri) {
        return setProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG, clusterUri);
    }

    /**
     * Sets the consumer group id of the PscSource.
     *
     * @param groupId the group id of the PscSource.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setGroupId(String groupId) {
        return setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, groupId);
    }

    /**
     * Set a list of topicRns the PscSource should consume from. All the topics in the list should
     * have existed in the PubSub cluster. Otherwise an exception will be thrown. To allow some of
     * the topics to be created lazily, please use {@link #setTopicUriPattern(Pattern)} instead.
     *
     * @param topicUris the list of topicRns to consume from.
     * @return this PscSourceBuilder.
     * @see com.pinterest.psc.consumer.PscConsumer#subscribe(Collection)
     */
    public PscSourceBuilder<OUT> setTopicUris(List<String> topicUris) {
        ensureSubscriberIsNull("topicUris");
        subscriber = PscSubscriber.getTopicUriListSubscriber(topicUris);
        return this;
    }

    /**
     * Set a list of topicRns the PscSource should consume from. All the topics in the list should
     * have existed in the PubSub cluster. Otherwise an exception will be thrown. To allow some of
     * the topics to be created lazily, please use {@link #setTopicUriPattern(Pattern)} instead.
     *
     * @param topicUris the list of topicRns to consume from.
     * @return this PscSourceBuilder.
     * @see com.pinterest.psc.consumer.PscConsumer#subscribe(Collection)
     */
    public PscSourceBuilder<OUT> setTopicUris(String... topicUris) {
        return setTopicUris(Arrays.asList(topicUris));
    }

    /**
     * Set a topic pattern to consume from use the java {@link Pattern}.
     *
     * @param topicUriPattern the pattern of the topic name to consume from.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setTopicUriPattern(Pattern topicUriPattern) {
        ensureSubscriberIsNull("topicUri pattern");
        subscriber = PscSubscriber.getTopicPatternSubscriber(topicUriPattern);
        return this;
    }

    /**
     * Set a set of partitions to consume from.
     *
     * @param partitions the set of partitions to consume from.
     * @return this PscSourceBuilder.
     * @see com.pinterest.psc.consumer.PscConsumer#assign(Collection)
     */
    public PscSourceBuilder<OUT> setPartitions(Set<TopicUriPartition> partitions) {
        ensureSubscriberIsNull("partitions");
        subscriber = PscSubscriber.getPartitionSetSubscriber(partitions);
        return this;
    }

    /**
     * Set a custom Kafka subscriber to use to discover new splits.
     *
     * @param pscSubscriber the {@link PscSubscriber} to use for split discovery.
     * @return this KafkaSourceBuilder.
     */
    public PscSourceBuilder<OUT> setPscSubscriber(PscSubscriber pscSubscriber) {
        ensureSubscriberIsNull("custom");
        this.subscriber = checkNotNull(pscSubscriber);
        return this;
    }

    /**
     * Specify from which offsets the PscSource should start consume from by providing an {@link
     * OffsetsInitializer}.
     *
     * <p>The following {@link OffsetsInitializer}s are commonly used and provided out of the box.
     * Users can also implement their own {@link OffsetsInitializer} for custom behaviors.
     *
     * <ul>
     *   <li>{@link OffsetsInitializer#earliest()} - starting from the earliest offsets. This is
     *       also the default {@link OffsetsInitializer} of the PscSource for starting offsets.
     *   <li>{@link OffsetsInitializer#latest()} - starting from the latest offsets.
     *   <li>{@link OffsetsInitializer#committedOffsets()} - starting from the committed offsets of
     *       the consumer group.
     *   <li>{@link
     *       OffsetsInitializer#committedOffsets(String)}
     *       - starting from the committed offsets of the consumer group. If there is no committed
     *       offsets, starting from the offsets specified by the offset reset strategy.
     *   <li>{@link OffsetsInitializer#offsets(Map)} - starting from the specified offsets for each
     *       partition.
     *   <li>{@link OffsetsInitializer#timestamp(long)} - starting from the specified timestamp for
     *       each partition. Note that the guarantee here is that all the records in the backend whose
     *       {@link MessageId#getOffset()} is greater than
     *       the given starting timestamp will be consumed. However, it is possible that some
     *       consumer records whose timestamp is smaller than the given starting timestamp are also
     *       consumed.
     * </ul>
     *
     * @param startingOffsetsInitializer the {@link OffsetsInitializer} setting the starting offsets
     *     for the Source.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setStartingOffsets(
            OffsetsInitializer startingOffsetsInitializer) {
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        return this;
    }

    /**
     * By default the PscSource is set to run as {@link Boundedness#CONTINUOUS_UNBOUNDED} and thus
     * never stops until the Flink job fails or is canceled. To let the PscSource run as a
     * streaming source but still stop at some point, one can set an {@link OffsetsInitializer} to
     * specify the stopping offsets for each partition. When all the partitions have reached their
     * stopping offsets, the PscSource will then exit.
     *
     * <p>This method is different from {@link #setBounded(OffsetsInitializer)} in that after
     * setting the stopping offsets with this method, {@link PscSource#getBoundedness()} will
     * still return {@link Boundedness#CONTINUOUS_UNBOUNDED} even though it will stop at the
     * stopping offsets specified by the stopping offsets {@link OffsetsInitializer}.
     *
     * <p>The following {@link OffsetsInitializer} are commonly used and provided out of the box.
     * Users can also implement their own {@link OffsetsInitializer} for custom behaviors.
     *
     * <ul>
     *   <li>{@link OffsetsInitializer#latest()} - stop at the latest offsets of the partitions when
     *       the PscSource starts to run.
     *   <li>{@link OffsetsInitializer#committedOffsets()} - stops at the committed offsets of the
     *       consumer group.
     *   <li>{@link OffsetsInitializer#offsets(Map)} - stops at the specified offsets for each
     *       partition.
     *   <li>{@link OffsetsInitializer#timestamp(long)} - stops at the specified timestamp for each
     *       partition. The guarantee of setting the stopping timestamp is that no records
     *       whose {@link MessageId#getTimestamp()} is greater
     *       than the given stopping timestamp will be consumed. However, it is possible that some
     *       records whose timestamp is smaller than the specified stopping timestamp are not
     *       consumed.
     * </ul>
     *
     * @param stoppingOffsetsInitializer The {@link OffsetsInitializer} to specify the stopping
     *     offset.
     * @return this PscSourceBuilder.
     * @see #setBounded(OffsetsInitializer)
     */
    public PscSourceBuilder<OUT> setUnbounded(OffsetsInitializer stoppingOffsetsInitializer) {
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        return this;
    }

    /**
     * By default the PscSource is set to run as {@link Boundedness#CONTINUOUS_UNBOUNDED} and thus
     * never stops until the Flink job fails or is canceled. To let the PscSource run in
     * {@link Boundedness#BOUNDED} and stop at some point, one can set an {@link
     * OffsetsInitializer} to specify the stopping offsets for each partition. When all the
     * partitions have reached their stopping offsets, the PscSource will then exit.
     *
     * <p>This method is different from {@link #setUnbounded(OffsetsInitializer)} that after setting
     * the stopping offsets with this method, {@link PscSource#getBoundedness()} will return
     * {@link Boundedness#BOUNDED} instead of {@link Boundedness#CONTINUOUS_UNBOUNDED}.
     *
     * <p>The following {@link OffsetsInitializer} are commonly used and provided out of the box.
     * Users can also implement their own {@link OffsetsInitializer} for custom behaviors.
     *
     * <ul>
     *   <li>{@link OffsetsInitializer#latest()} - stop at the latest offsets of the partitions when
     *       the PscSource starts to run.
     *   <li>{@link OffsetsInitializer#committedOffsets()} - stops at the committed offsets of the
     *       consumer group.
     *   <li>{@link OffsetsInitializer#offsets(Map)} - stops at the specified offsets for each
     *       partition.
     *   <li>{@link OffsetsInitializer#timestamp(long)} - stops at the specified timestamp for each
     *       partition. The guarantee of setting the stopping timestamp is that no records
     *       whose {@link MessageId#getTimestamp()} is greater
     *       than the given stopping timestamp will be consumed. However, it is possible that some
     *       records whose timestamp is smaller than the specified stopping timestamp are not
     *       consumed.
     * </ul>
     *
     * @param stoppingOffsetsInitializer the {@link OffsetsInitializer} to specify the stopping
     *     offsets.
     * @return this PscSourceBuilder.
     * @see #setUnbounded(OffsetsInitializer)
     */
    public PscSourceBuilder<OUT> setBounded(OffsetsInitializer stoppingOffsetsInitializer) {
        this.boundedness = Boundedness.BOUNDED;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        return this;
    }

    /**
     * Sets the {@link PscRecordDeserializationSchema deserializer} of the {@link
     * org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} for PscSource.
     *
     * @param recordDeserializer the deserializer for PSC {@link
     *     org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord}.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setDeserializer(
            PscRecordDeserializationSchema<OUT> recordDeserializer) {
        this.deserializationSchema = recordDeserializer;
        return this;
    }

    /**
     * Sets the {@link PscRecordDeserializationSchema deserializer} of the {@link
     * org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} for PscSource. The given
     * {@link DeserializationSchema} will be used to deserialize the value of ConsumerRecord. The
     * other information (e.g. key) in a ConsumerRecord will be ignored.
     *
     * @param deserializationSchema the {@link DeserializationSchema} to use for deserialization.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setValueOnlyDeserializer(
            DeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema =
                PscRecordDeserializationSchema.valueOnly(deserializationSchema);
        return this;
    }

    /**
     * Sets the client id prefix of this PscSource.
     *
     * @param prefix the client id prefix to use for this PscSource.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setClientIdPrefix(String prefix) {
        return setProperty(PscSourceOptions.CLIENT_ID_PREFIX.key(), prefix);
    }

    /**
     * Set the clientRackId supplier to be passed down to the KafkaPartitionSplitReader.
     *
     * @param rackIdCallback callback to provide Kafka consumer client.rack
     * @return this KafkaSourceBuilder
     */
    public PscSourceBuilder<OUT> setRackIdSupplier(SerializableSupplier<String> rackIdCallback) {
        this.rackIdSupplier = rackIdCallback;
        return this;
    }

    /**
     * Set an arbitrary property for the PscSource and PscConsumer. The valid keys can be found
     * in {@link PscConfiguration} and {@link PscSourceOptions}.
     *
     * <p>Note that the following keys will be overridden by the builder when the PscSource is
     * created.
     *
     * <ul>
     *   <li><code>key.deserializer</code> is always set to {@link com.pinterest.psc.serde.ByteArrayDeserializer}.
     *   <li><code>value.deserializer</code> is always set to {@link com.pinterest.psc.serde.ByteArrayDeserializer}.
     *   <li><code>auto.offset.reset.strategy</code> is overridden by {@link
     *       OffsetsInitializer#getAutoOffsetResetStrategy()} for the starting offsets, which is by
     *       default {@link OffsetsInitializer#earliest()}.
     *   <li><code>partition.discovery.interval.ms</code> is overridden to -1 when {@link
     *       #setBounded(OffsetsInitializer)} has been invoked.
     * </ul>
     *
     * @param key the key of the property.
     * @param value the value of the property.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setProperty(String key, String value) {
        props.setProperty(key, value);
        return this;
    }

    /**
     * Set arbitrary properties for the PscSource and PscConsumer. The valid keys can be found
     * in {@link PscConfiguration} and {@link PscSourceOptions}.
     *
     * <p>Note that the following keys will be overridden by the builder when the PscSource is
     * created.
     *
     * <ul>
     *   <li><code>key.deserializer</code> is always set to {@link com.pinterest.psc.serde.ByteArrayDeserializer}.
     *   <li><code>value.deserializer</code> is always set to {@link com.pinterest.psc.serde.ByteArrayDeserializer}.
     *   <li><code>auto.offset.reset.strategy</code> is overridden by {@link
     *       OffsetsInitializer#getAutoOffsetResetStrategy()} for the starting offsets, which is by
     *       default {@link OffsetsInitializer#earliest()}.
     *   <li><code>partition.discovery.interval.ms</code> is overridden to -1 when {@link
     *       #setBounded(OffsetsInitializer)} has been invoked.
     *   <li><code>client.id</code> is overridden to the "client.id.prefix-RANDOM_LONG", or
     *       "group.id-RANDOM_LONG" if the client id prefix is not set.
     * </ul>
     *
     * @param props the properties to set for the PscSource.
     * @return this PscSourceBuilder.
     */
    public PscSourceBuilder<OUT> setProperties(Properties props) {
        this.props.putAll(props);
        return this;
    }

    /**
     * Build the {@link PscSource}.
     *
     * @return a PscSource with the settings made for this builder.
     */
    public PscSource<OUT> build() {
        sanityCheck();
        parseAndSetRequiredProperties();
        return new PscSource<>(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                deserializationSchema,
                props,
                rackIdSupplier);
    }

    // ------------- private helpers  --------------

    private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
        if (subscriber != null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot use %s for consumption because a %s is already set for consumption.",
                            attemptingSubscribeMode, subscriber.getClass().getSimpleName()));
        }
    }

    private void parseAndSetRequiredProperties() {
        maybeOverride(
                PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER,
                ByteArrayDeserializer.class.getName(),
                true);
        maybeOverride(
                PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER,
                ByteArrayDeserializer.class.getName(),
                true);
        if (!props.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID)) {
            LOG.warn(
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

        // If the client id prefix is not set, reuse the consumer group id as the client id prefix,
        // or generate a random string if consumer group id is not specified.
        maybeOverride(
                PscSourceOptions.CLIENT_ID_PREFIX.key(),
                props.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID)
                        ? props.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID)
                        : "PscSource-" + new Random().nextLong(),
                false);
    }

    private boolean maybeOverride(String key, String value, boolean override) {
        boolean overridden = false;
        String userValue = props.getProperty(key);
        if (userValue != null) {
            if (override) {
                LOG.warn(
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
        // Check required configs.
        for (String requiredConfig : REQUIRED_CONFIGS) {
            checkNotNull(
                    props.getProperty(requiredConfig),
                    String.format("Property %s is required but not provided", requiredConfig));
        }
        // Check required settings.
        checkNotNull(
                subscriber,
                "No subscribe mode is specified, "
                        + "should be one of topics, topic pattern and partition set.");
        checkNotNull(deserializationSchema, "Deserialization schema is required but not provided.");
        // Check consumer group ID
        checkState(
                props.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID) || !offsetCommitEnabledManually(),
                String.format(
                        "Property %s is required when offset commit is enabled",
                        PscConfiguration.PSC_CONSUMER_GROUP_ID));
        // Check offsets initializers
        if (startingOffsetsInitializer instanceof OffsetsInitializerValidator) {
            ((OffsetsInitializerValidator) startingOffsetsInitializer).validate(props);
        }
        if (stoppingOffsetsInitializer instanceof OffsetsInitializerValidator) {
            ((OffsetsInitializerValidator) stoppingOffsetsInitializer).validate(props);
        }
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
}
