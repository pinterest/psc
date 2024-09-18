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

import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.serde.StringDeserializer;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.util.TestLoggerExtension;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link PscSourceBuilder}. */
@ExtendWith(TestLoggerExtension.class)
public class PscSourceBuilderTest {

    @Test
    public void testBuildSourceWithGroupId() {
        final PscSource<String> pscSource = getBasicBuilder().setGroupId("groupId").build();
        // Commit on checkpoint should be enabled by default
        Assertions.assertTrue(
                pscSource
                        .getConfiguration()
                        .get(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT));
        // Auto commit should be disabled by default
        Assertions.assertFalse(
                pscSource
                        .getConfiguration()
                        .get(
                                ConfigOptions.key(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED)
                                        .booleanType()
                                        .noDefaultValue()));
    }

    @Test
    public void testBuildSourceWithoutGroupId() {
        final PscSource<String> pscSource = getBasicBuilder().build();
        // Commit on checkpoint and auto commit should be disabled because group.id is not specified
        Assertions.assertFalse(
                pscSource
                        .getConfiguration()
                        .get(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT));
        Assertions.assertFalse(
                pscSource
                        .getConfiguration()
                        .get(
                                ConfigOptions.key(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED)
                                        .booleanType()
                                        .noDefaultValue()));
    }

    @Test
    public void testEnableCommitOnCheckpointWithoutGroupId() {
        final IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                getBasicBuilder()
                                        .setProperty(
                                                PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT
                                                        .key(),
                                                "true")
                                        .build());
        MatcherAssert.assertThat(
                exception.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required when offset commit is enabled"));
    }

    @Test
    public void testEnableAutoCommitWithoutGroupId() {
        final IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                getBasicBuilder()
                                        .setProperty(
                                                PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "true")
                                        .build());
        MatcherAssert.assertThat(
                exception.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required when offset commit is enabled"));
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
        final IllegalStateException startingOffsetException =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                getBasicBuilder()
                                        .setStartingOffsets(OffsetsInitializer.committedOffsets())
                                        .build());
        MatcherAssert.assertThat(
                startingOffsetException.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required when using committed offset for offsets initializer"));

        // Using OffsetsInitializer#committedOffsets as stopping offsets
        final IllegalStateException stoppingOffsetException =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                getBasicBuilder()
                                        .setBounded(OffsetsInitializer.committedOffsets())
                                        .build());
        MatcherAssert.assertThat(
                stoppingOffsetException.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required when using committed offset for offsets initializer"));

        // Using OffsetsInitializer#offsets to manually specify committed offset as starting offset
        final IllegalStateException specificStartingOffsetException =
                assertThrows(
                        IllegalStateException.class,
                        () -> {
                            final Map<TopicUriPartition, Long> offsetMap = new HashMap<>();
                            offsetMap.put(
                                    new TopicUriPartition("topic", 0),
                                    PscTopicUriPartitionSplit.COMMITTED_OFFSET);
                            getBasicBuilder()
                                    .setStartingOffsets(OffsetsInitializer.offsets(offsetMap))
                                    .build();
                        });
        MatcherAssert.assertThat(
                specificStartingOffsetException.getMessage(),
                CoreMatchers.containsString(
                        "Property group.id is required because partition topic-0 is initialized with committed offset"));
    }

    private PscSourceBuilder<String> getBasicBuilder() {
        return new PscSourceBuilder<String>()
                .setTopicUris("topic")
                .setDeserializer(
                        PscRecordDeserializationSchema.valueOnly(StringDeserializer.class));
    }
}
