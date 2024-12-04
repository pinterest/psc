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

package com.pinterest.flink.connector.psc.source;

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriber;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import com.pinterest.psc.serde.StringDeserializer;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.util.TestLoggerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PscSourceBuilder}. */
@ExtendWith(TestLoggerExtension.class)
public class PscSourceBuilderTest {

    @Test
    public void testBuildSourceWithGroupId() {
        final PscSource<String> pscSource = getBasicBuilder().setGroupId("groupId").build();
        // Commit on checkpoint should be enabled by default
        assertThat(
                pscSource
                        .getConfiguration()
                        .get(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT))
                .isTrue();
        // Auto commit should be disabled by default
        assertThat(
                pscSource
                        .getConfiguration()
                        .get(
                                ConfigOptions.key(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED)
                                        .booleanType()
                                        .noDefaultValue()))
                .isFalse();
    }

    @Test
    public void testBuildSourceWithoutGroupId() {
        final PscSource<String> pscSource = getBasicBuilder().build();
        // Commit on checkpoint and auto commit should be disabled because group.id is not specified
        assertThat(
                pscSource
                        .getConfiguration()
                        .get(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT))
                .isFalse();
        assertThat(
                pscSource
                        .getConfiguration()
                        .get(
                                ConfigOptions.key(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED)
                                        .booleanType()
                                        .noDefaultValue()))
                .isFalse();
    }

    @Test
    public void testEnableCommitOnCheckpointWithoutGroupId() {
        assertThatThrownBy(
                        () ->
                                getBasicBuilder()
                                        .setProperty(
                                                PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT
                                                        .key(),
                                                "true")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property psc.consumer.group.id is required when offset commit is enabled");
    }

    @Test
    public void testEnableAutoCommitWithoutGroupId() {
        assertThatThrownBy(
                        () ->
                                getBasicBuilder()
                                        .setProperty(
                                                PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "true")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property psc.consumer.group.id is required when offset commit is enabled");
    }

    @Test
    public void testDisableOffsetCommitWithoutGroupId() {
        getBasicBuilder()
                .setProperty(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false")
                .build();
        getBasicBuilder().setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false").build();
    }

    @Test
    public void testUsingCommittedOffsetsInitializerWithoutGroupId() {
        // Using OffsetsInitializer#committedOffsets as starting offsets
        assertThatThrownBy(
                        () ->
                                getBasicBuilder()
                                        .setStartingOffsets(OffsetsInitializer.committedOffsets())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property psc.consumer.group.id is required when using committed offset for offsets initializer");

        // Using OffsetsInitializer#committedOffsets as stopping offsets
        assertThatThrownBy(
                        () ->
                                getBasicBuilder()
                                        .setBounded(OffsetsInitializer.committedOffsets())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Property psc.consumer.group.id is required when using committed offset for offsets initializer");

        // Using OffsetsInitializer#offsets to manually specify committed offset as starting offset
        TopicUriPartition tup = new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + "topic", 0);
        assertThatThrownBy(
                        () -> {
                            final Map<TopicUriPartition, Long> offsetMap = new HashMap<>();
                            offsetMap.put(
                                    tup,
                                    PscTopicUriPartitionSplit.COMMITTED_OFFSET);
                            getBasicBuilder()
                                    .setStartingOffsets(OffsetsInitializer.offsets(offsetMap))
                                    .build();
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Property psc.consumer.group.id is required because partition " + tup + " is initialized with committed offset");
    }

    @Test
    public void testSettingCustomKafkaSubscriber() {
        ExampleCustomSubscriber exampleCustomSubscriber = new ExampleCustomSubscriber();
        PscSourceBuilder<String> customPscSubscriberBuilder =
                new PscSourceBuilder<String>()
                        .setPscSubscriber(exampleCustomSubscriber)
                        .setDeserializer(
                                PscRecordDeserializationSchema.valueOnly(
                                        StringDeserializer.class));

        assertThat(customPscSubscriberBuilder.build().getPscSubscriber())
                .isEqualTo(exampleCustomSubscriber);

        assertThatThrownBy(() -> customPscSubscriberBuilder.setTopicUris(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + "topic"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot use topics for consumption because a ExampleCustomSubscriber is already set for consumption.");

        assertThatThrownBy(
                () -> customPscSubscriberBuilder.setTopicUriPattern(Pattern.compile(".+")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot use topic pattern for consumption because a ExampleCustomSubscriber is already set for consumption.");

        assertThatThrownBy(
                () ->
                        customPscSubscriberBuilder.setPartitions(
                                Collections.singleton(new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + "topic", 0))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot use partitions for consumption because a ExampleCustomSubscriber is already set for consumption.");
    }

    private PscSourceBuilder<String> getBasicBuilder() {
        return new PscSourceBuilder<String>()
                .setTopicUris(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + "topic")
                .setProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX)
                .setDeserializer(
                        PscRecordDeserializationSchema.valueOnly(StringDeserializer.class));
    }

    private static class ExampleCustomSubscriber implements PscSubscriber {

        @Override
        public Set<TopicUriPartition> getSubscribedTopicUriPartitions(PscMetadataClient pscMetadataClient, TopicUri topicUri) {
            return Collections.singleton(new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + "topic", 0));
        }
    }
}
