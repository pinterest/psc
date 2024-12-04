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

package com.pinterest.flink.connector.psc.source.enumerator.initializer;

import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumerator;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.connector.psc.testutils.PscSourceTestEnv;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUriPartition;

import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


/** Unit tests for {@link OffsetsInitializer}. */
public class OffsetsInitializerTest {
    private static final String TOPIC = "topic";
    private static final String TOPIC_URI = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + TOPIC;
    private static final String TOPIC2 = "topic2";
    private static final String TOPIC_URI2 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + TOPIC2;
    private static final String EMPTY_TOPIC3 = "topic3";
    private static final String EMPTY_TOPIC_URI3 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + EMPTY_TOPIC3;
    private static PscSourceEnumerator.PartitionOffsetsRetrieverImpl retriever;

    @BeforeClass
    public static void setup() throws Throwable {
        PscSourceTestEnv.setup();
        PscSourceTestEnv.setupTopic(TOPIC_URI, true, true, PscSourceTestEnv::getRecordsForTopic);
        PscSourceTestEnv.setupTopic(TOPIC_URI2, false, false, PscSourceTestEnv::getRecordsForTopic);
        PscSourceTestEnv.createTestTopic(EMPTY_TOPIC_URI3);
        retriever =
                new PscSourceEnumerator.PartitionOffsetsRetrieverImpl(
                        PscSourceTestEnv.getMetadataClient(), PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER_URI, PscSourceTestEnv.GROUP_ID);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        retriever.close();
        PscSourceTestEnv.tearDown();
    }

    @Test
    public void testEarliestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.earliest();
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC_URI);
        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertThat(offsets).hasSameSizeAs(partitions);
        assertThat(offsets.keySet()).containsAll(partitions);
        for (long offset : offsets.values()) {
            assertThat(offset).isEqualTo(PscTopicUriPartitionSplit.EARLIEST_OFFSET);
        }
        assertThat(initializer.getAutoOffsetResetStrategy())
                .isEqualTo(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
    }

    @Test
    public void testLatestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.latest();
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC_URI);
        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertThat(offsets).hasSameSizeAs(partitions);
        assertThat(offsets.keySet()).containsAll(partitions);
        for (long offset : offsets.values()) {
            assertThat(offset).isEqualTo(PscTopicUriPartitionSplit.LATEST_OFFSET);
        }
        assertThat(initializer.getAutoOffsetResetStrategy())
                .isEqualTo(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST);
    }

    @Test
    public void testCommittedGroupOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.committedOffsets();
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC_URI);
        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertThat(offsets).hasSameSizeAs(partitions);
        offsets.forEach(
                (tp, offset) -> assertThat((long) offset).isEqualTo(PscTopicUriPartitionSplit.COMMITTED_OFFSET));
        assertThat(initializer.getAutoOffsetResetStrategy()).isEqualTo(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE);
    }

    @Test
    public void testTimestampOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.timestamp(2001);
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC_URI);
        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        offsets.forEach(
                (tp, offset) -> {
                    long expectedOffset = tp.getPartition() > 2 ? tp.getPartition() : 3L;
                    assertThat((long) offset).isEqualTo(expectedOffset);
                });
        assertThat(initializer.getAutoOffsetResetStrategy()).isEqualTo(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST);
    }

    @Test
    public void testTimestampOffsetsInitializerForEmptyPartitions() {
        OffsetsInitializer initializer = OffsetsInitializer.timestamp(2001);
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(EMPTY_TOPIC3);
        Map<TopicUriPartition, Long> expectedOffsets =
                partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> 0L));
        assertThat(initializer.getPartitionOffsets(partitions, retriever))
                .as("offsets are equal to 0 since the timestamp is out of range.")
                .isEqualTo(expectedOffsets);
        assertThat(initializer.getAutoOffsetResetStrategy()).isEqualTo(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST);
    }

    @Test
    public void testSpecificOffsetsInitializer() throws TopicUriSyntaxException {
        Map<TopicUriPartition, Long> specifiedOffsets = new HashMap<>();
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC_URI);
        Map<TopicUriPartition, Long> committedOffsets =
                PscSourceTestEnv.getCommittedOffsets(partitions);
        partitions.forEach(tp -> specifiedOffsets.put(tp, (long) tp.getPartition()));
        // Remove the specified offsets for partition 0.
        TopicUriPartition partitionSetToCommitted = new TopicUriPartition(TOPIC_URI, 0);
        specifiedOffsets.remove(partitionSetToCommitted);
        OffsetsInitializer initializer = OffsetsInitializer.offsets(specifiedOffsets);

        assertThat(initializer.getAutoOffsetResetStrategy()).isEqualTo(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        // The partition without committed offset should fallback to offset reset strategy.
        TopicUriPartition partitionSetToEarliest = new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(TOPIC_URI2)), 0);
        partitions.add(partitionSetToEarliest);

        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        for (TopicUriPartition tp : partitions) {
            Long offset = offsets.get(tp);
            long expectedOffset;
            if (tp.equals(partitionSetToCommitted)) {
                expectedOffset = committedOffsets.get(tp);
            } else if (tp.equals(partitionSetToEarliest)) {
                expectedOffset = 0L;
            } else {
                expectedOffset = specifiedOffsets.get(tp);
            }
            assertThat((long) offset)
                    .as(String.format("%s has incorrect offset.", tp))
                    .isEqualTo(expectedOffset);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSpecifiedOffsetsInitializerWithoutOffsetResetStrategy() {
        OffsetsInitializer initializer =
                OffsetsInitializer.offsets(Collections.emptyMap(), PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE);
        initializer.getPartitionOffsets(PscSourceTestEnv.getPartitionsForTopic(TOPIC_URI), retriever);
    }
}
