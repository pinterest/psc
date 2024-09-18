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

package com.pinterest.flink.connector.psc.source.enumerator;

import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplitSerializer;
import com.pinterest.psc.common.TopicUriPartition;
import org.apache.flink.connector.base.source.utils.SerdeUtils;

import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/** Test for {@link PscSourceEnumStateSerializer}. */
public class PscSourceEnumStateSerializerTest {

    private static final int NUM_READERS = 10;
    private static final String TOPIC_PREFIX = "topic-";
    private static final int NUM_PARTITIONS_PER_TOPIC = 10;
    private static final long STARTING_OFFSET = PscTopicUriPartitionSplit.EARLIEST_OFFSET;

    @Test
    public void testEnumStateSerde() throws IOException {
        final PscSourceEnumState state = new PscSourceEnumState(constructTopicPartitions());
        final PscSourceEnumStateSerializer serializer = new PscSourceEnumStateSerializer();

        final byte[] bytes = serializer.serialize(state);

        final PscSourceEnumState restoredState =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(state.assignedPartitions(), restoredState.assignedPartitions());
    }

    @Test
    public void testBackwardCompatibility() throws IOException {

        final Set<TopicUriPartition> topicPartitions = constructTopicPartitions();
        final Map<Integer, Set<PscTopicUriPartitionSplit>> splitAssignments =
                toSplitAssignments(topicPartitions);

        // Create bytes in the way of KafkaEnumStateSerializer version 0 doing serialization
        final byte[] bytes =
                SerdeUtils.serializeSplitAssignments(
                        splitAssignments, new PscTopicUriPartitionSplitSerializer());

        // Deserialize above bytes with KafkaEnumStateSerializer version 1 to check backward
        // compatibility
        final PscSourceEnumState kafkaSourceEnumState =
                new PscSourceEnumStateSerializer().deserialize(0, bytes);

        assertEquals(topicPartitions, kafkaSourceEnumState.assignedPartitions());
    }

    private Set<TopicUriPartition> constructTopicPartitions() {
        // Create topic partitions for readers.
        // Reader i will be assigned with NUM_PARTITIONS_PER_TOPIC splits, with topic name
        // "topic-{i}" and
        // NUM_PARTITIONS_PER_TOPIC partitions.
        // Totally NUM_READERS * NUM_PARTITIONS_PER_TOPIC partitions will be created.
        Set<TopicUriPartition> topicPartitions = new HashSet<>();
        for (int readerId = 0; readerId < NUM_READERS; readerId++) {
            for (int partition = 0; partition < NUM_PARTITIONS_PER_TOPIC; partition++) {
                topicPartitions.add(new TopicUriPartition(TOPIC_PREFIX + readerId, partition));
            }
        }
        return topicPartitions;
    }

    private Map<Integer, Set<PscTopicUriPartitionSplit>> toSplitAssignments(
            Collection<TopicUriPartition> topicPartitions) {
        // Assign splits to readers according to topic name. For example, topic "topic-5" will be
        // assigned to reader with ID=5
        Map<Integer, Set<PscTopicUriPartitionSplit>> splitAssignments = new HashMap<>();
        topicPartitions.forEach(
                (tp) ->
                        splitAssignments
                                .computeIfAbsent(
                                        Integer.valueOf(
                                                tp.getTopicUriAsString().substring(TOPIC_PREFIX.length())),
                                        HashSet::new)
                                .add(new PscTopicUriPartitionSplit(tp, STARTING_OFFSET)));
        return splitAssignments;
    }
}
