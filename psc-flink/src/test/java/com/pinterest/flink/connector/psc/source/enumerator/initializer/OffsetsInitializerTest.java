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
import com.pinterest.psc.common.TopicUriPartition;

import com.pinterest.psc.config.PscConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link OffsetsInitializer}. */
public class OffsetsInitializerTest {
    private static final String TOPIC = "topic";
    private static final String TOPIC2 = "topic2";
    private static PscSourceEnumerator.PartitionOffsetsRetrieverImpl retriever;

    @BeforeClass
    public static void setup() throws Throwable {
        PscSourceTestEnv.setup();
        PscSourceTestEnv.setupTopic(TOPIC, true, true, PscSourceTestEnv::getRecordsForTopic);
        PscSourceTestEnv.setupTopic(TOPIC2, false, false, PscSourceTestEnv::getRecordsForTopic);
        retriever =
                new PscSourceEnumerator.PartitionOffsetsRetrieverImpl(
                        PscSourceTestEnv.getAdminClient(), clusterUri, PscSourceTestEnv.GROUP_ID);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        retriever.close();
        PscSourceTestEnv.tearDown();
    }

    @Test
    public void testEarliestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.earliest();
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        assertTrue(offsets.keySet().containsAll(partitions));
        for (long offset : offsets.values()) {
            Assert.assertEquals(PscTopicUriPartitionSplit.EARLIEST_OFFSET, offset);
        }
        assertEquals(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testLatestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.latest();
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        assertTrue(offsets.keySet().containsAll(partitions));
        for (long offset : offsets.values()) {
            assertEquals(PscTopicUriPartitionSplit.LATEST_OFFSET, offset);
        }
        assertEquals(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testCommittedGroupOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.committedOffsets();
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        offsets.forEach(
                (tp, offset) -> assertEquals(PscTopicUriPartitionSplit.COMMITTED_OFFSET, (long) offset));
        assertEquals(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testTimestampOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.timestamp(2001);
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicUriPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        offsets.forEach(
                (tp, offset) -> {
                    long expectedOffset = tp.getPartition() > 2 ? tp.getPartition() : 3L;
                    assertEquals(expectedOffset, (long) offset);
                });
        assertEquals(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testSpecificOffsetsInitializer() {
        Map<TopicUriPartition, Long> specifiedOffsets = new HashMap<>();
        List<TopicUriPartition> partitions = PscSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicUriPartition, Long> committedOffsets =
                PscSourceTestEnv.getCommittedOffsets(partitions);
        partitions.forEach(tp -> specifiedOffsets.put(tp, (long) tp.getPartition()));
        // Remove the specified offsets for partition 0.
        TopicUriPartition partitionSetToCommitted = new TopicUriPartition(TOPIC, 0);
        specifiedOffsets.remove(partitionSetToCommitted);
        OffsetsInitializer initializer = OffsetsInitializer.offsets(specifiedOffsets);

        assertEquals(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST, initializer.getAutoOffsetResetStrategy());
        // The partition without committed offset should fallback to offset reset strategy.
        TopicUriPartition partitionSetToEarliest = new TopicUriPartition(TOPIC2, 0);
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
            assertEquals(
                    String.format("%s has incorrect offset.", tp), expectedOffset, (long) offset);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSpecifiedOffsetsInitializerWithoutOffsetResetStrategy() {
        OffsetsInitializer initializer =
                OffsetsInitializer.offsets(Collections.emptyMap(), PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE);
        initializer.getPartitionOffsets(PscSourceTestEnv.getPartitionsForTopic(TOPIC), retriever);
    }
}
