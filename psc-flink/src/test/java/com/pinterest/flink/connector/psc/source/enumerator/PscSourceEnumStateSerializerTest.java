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
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import org.apache.flink.connector.base.source.utils.SerdeUtils;

import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PscSourceEnumStateSerializer}. */
public class PscSourceEnumStateSerializerTest {

    private static final int NUM_READERS = 10;
    private static final String TOPIC_PREFIX = "topic-";
    private static final String TOPIC_URI_PREFIX = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + TOPIC_PREFIX;
    private static final int NUM_PARTITIONS_PER_TOPIC = 10;
    private static final long STARTING_OFFSET = PscTopicUriPartitionSplit.EARLIEST_OFFSET;

    @Test
    public void testEnumStateSerde() throws IOException {
        final PscSourceEnumState state = new PscSourceEnumState(
                constructPscTopicUriPartitions(0),
                constructPscTopicUriPartitions(NUM_PARTITIONS_PER_TOPIC),
                true
        );
        final PscSourceEnumStateSerializer serializer = new PscSourceEnumStateSerializer();

        final byte[] bytes = serializer.serialize(state);

        final PscSourceEnumState restoredState =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restoredState.assignedPartitions()).isEqualTo(state.assignedPartitions());
        assertThat(restoredState.unassignedInitialPartitions())
                .isEqualTo(state.unassignedInitialPartitions());
        assertThat(restoredState.initialDiscoveryFinished()).isTrue();
    }

    @Test
    public void testBackwardCompatibilityWithFlinkKafka() throws IOException {

        final Set<TopicPartition> kafkaTopicPartitions = constructKafkaTopicPartitions(0);
        final Map<Integer, Set<KafkaPartitionSplit>> splitAssignments =
                toKafkaSplitAssignments(kafkaTopicPartitions);

        // Create bytes in the way of KafkaEnumStateSerializer version 0 doing serialization
        final byte[] bytesV0 =
                SerdeUtils.serializeSplitAssignments(
                        splitAssignments, new KafkaPartitionSplitSerializer());
        // Create bytes in the way of KafkaEnumStateSerializer version 1 doing serialization
        final byte[] bytesV1 =
                KafkaSourceEnumStateSerializer.serializeTopicPartitions(kafkaTopicPartitions);

        // Deserialize above bytes with PscEnumStateSerializer version 100,000 to check backward
        // compatibility and recoverability from state serialized by KafkaEnumStateSerializer
        PscSourceEnumStateSerializer pscSourceEnumStateSerializer = new PscSourceEnumStateSerializer();
        pscSourceEnumStateSerializer.setClusterUri(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        final PscSourceEnumState pscSourceEnumStateV0 =
                pscSourceEnumStateSerializer.deserialize(0, bytesV0);
        final PscSourceEnumState pscSourceEnumStateV1 =
                pscSourceEnumStateSerializer.deserialize(1, bytesV1);

        Set<TopicPartition> convertedTopicPartitionsV0 = pscSourceEnumStateV0.assignedPartitions().stream().map(tup -> new TopicPartition(tup.getTopicUri().getTopic(), tup.getPartition())).collect(Collectors.toSet());
        Set<TopicPartition> convertedTopicPartitionsV1 = pscSourceEnumStateV1.assignedPartitions().stream().map(tup -> new TopicPartition(tup.getTopicUri().getTopic(), tup.getPartition())).collect(Collectors.toSet());

        assertThat(convertedTopicPartitionsV0).isEqualTo(kafkaTopicPartitions);
        assertThat(pscSourceEnumStateV0.unassignedInitialPartitions()).isEmpty();
        assertThat(pscSourceEnumStateV0.initialDiscoveryFinished()).isTrue();

        assertThat(convertedTopicPartitionsV1).isEqualTo(kafkaTopicPartitions);
        assertThat(pscSourceEnumStateV1.unassignedInitialPartitions()).isEmpty();
        assertThat(pscSourceEnumStateV1.initialDiscoveryFinished()).isTrue();

    }

    private Set<TopicUriPartition> constructPscTopicUriPartitions(int startPartition) {
        // Create topic partitions for readers.
        // Reader i will be assigned with NUM_PARTITIONS_PER_TOPIC splits, with topic name
        // "topic-{i}" and
        // NUM_PARTITIONS_PER_TOPIC partitions. The starting partition number is startPartition
        // Totally NUM_READERS * NUM_PARTITIONS_PER_TOPIC partitions will be created.
        Set<TopicUriPartition> topicPartitions = new HashSet<>();
        for (int readerId = 0; readerId < NUM_READERS; readerId++) {
            for (int partition = startPartition; partition < startPartition + NUM_PARTITIONS_PER_TOPIC; partition++) {
                topicPartitions.add(new TopicUriPartition(TOPIC_PREFIX + readerId, partition));
            }
        }
        return topicPartitions;
    }

    private Set<TopicPartition> constructKafkaTopicPartitions(int startPartition) {
        // Create topic partitions for readers.
        // Reader i will be assigned with NUM_PARTITIONS_PER_TOPIC splits, with topic name
        // "topic-{i}" and
        // NUM_PARTITIONS_PER_TOPIC partitions. The starting partition number is startPartition
        // Totally NUM_READERS * NUM_PARTITIONS_PER_TOPIC partitions will be created.
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int readerId = 0; readerId < NUM_READERS; readerId++) {
            for (int partition = startPartition;
                 partition < startPartition + NUM_PARTITIONS_PER_TOPIC;
                 partition++) {
                topicPartitions.add(new TopicPartition(TOPIC_PREFIX + readerId, partition));
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
                                                tp.getTopicUriAsString().substring(TOPIC_URI_PREFIX.length())),
                                        HashSet::new)
                                .add(new PscTopicUriPartitionSplit(tp, STARTING_OFFSET)));
        return splitAssignments;
    }

    private Map<Integer, Set<KafkaPartitionSplit>> toKafkaSplitAssignments(
            Collection<TopicPartition> topicPartitions) {
        // Assign splits to readers according to topic name. For example, topic "topic-5" will be
        // assigned to reader with ID=5
        Map<Integer, Set<KafkaPartitionSplit>> splitAssignments = new HashMap<>();
        topicPartitions.forEach(
                (tp) ->
                        splitAssignments
                                .computeIfAbsent(
                                        Integer.valueOf(
                                                tp.topic().substring(TOPIC_PREFIX.length())),
                                        HashSet::new)
                                .add(new KafkaPartitionSplit(tp, STARTING_OFFSET)));
        return splitAssignments;
    }
}
