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
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.utils.SerdeUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the enumerator
 * state of PSC source.
 */
@Internal
public class PscSourceEnumStateSerializer
        implements SimpleVersionedSerializer<PscSourceEnumState> {

    /**
     * state of VERSION_0 contains splitAssignments, which is a mapping from subtask ids to lists of
     * assigned splits.
     */
    private static final int VERSION_0 = 0;
    /** state of VERSION_1 only contains assignedPartitions, which is a list of assigned splits. */
    private static final int VERSION_1 = 1;
    /**
     * state of VERSION_2 contains initialDiscoveryFinished and partitions with different assignment
     * status.
     */
    private static final int VERSION_2 = 2;

    private static final int CURRENT_VERSION = VERSION_2;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PscSourceEnumState enumState) throws IOException {
        Set<TopicUriPartitionAndAssignmentStatus> partitions = enumState.partitions();
        boolean initialDiscoveryFinished = enumState.initialDiscoveryFinished();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(partitions.size());
            for (TopicUriPartitionAndAssignmentStatus topicPartitionAndAssignmentStatus : partitions) {
                out.writeUTF(topicPartitionAndAssignmentStatus.topicUriPartition().getTopicUriAsString());
                out.writeInt(topicPartitionAndAssignmentStatus.topicUriPartition().getPartition());
                out.writeInt(topicPartitionAndAssignmentStatus.assignmentStatus().getStatusCode());
            }
            out.writeBoolean(initialDiscoveryFinished);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PscSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case CURRENT_VERSION:
                return deserializeTopicPartitionAndAssignmentStatus(serialized);
            case VERSION_1:
                final Set<TopicUriPartition> assignedPartitions =
                        deserializeTopicPartitions(serialized);
                return new PscSourceEnumState(assignedPartitions, new HashSet<>(), true);
            case VERSION_0:
                Map<Integer, Set<PscTopicUriPartitionSplit>> currentPartitionAssignment =
                        SerdeUtils.deserializeSplitAssignments(
                                serialized, new PscTopicUriPartitionSplitSerializer(), HashSet::new);
                Set<TopicUriPartition> currentAssignedSplits = new HashSet<>();
                currentPartitionAssignment.forEach(
                        (reader, splits) ->
                                splits.forEach(
                                        split ->
                                                currentAssignedSplits.add(
                                                        split.getTopicUriPartition())));
                return new PscSourceEnumState(currentAssignedSplits, new HashSet<>(), true);
            default:
                throw new IOException(
                        String.format(
                                "The bytes are serialized with version %d, "
                                        + "while this deserializer only supports version up to %d",
                                version, CURRENT_VERSION));
        }
    }

    private static Set<TopicUriPartition> deserializeTopicPartitions(byte[] serializedTopicPartitions)
            throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedTopicPartitions);
             DataInputStream in = new DataInputStream(bais)) {

            final int numPartitions = in.readInt();
            Set<TopicUriPartition> topicPartitions = new HashSet<>(numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                final String topicUriString = in.readUTF();
                final int partition = in.readInt();
                topicPartitions.add(new TopicUriPartition(topicUriString, partition));
            }
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized topic partitions");
            }

            return topicPartitions;
        }
    }

    private static PscSourceEnumState deserializeTopicPartitionAndAssignmentStatus(
            byte[] serialized) throws IOException {

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {

            final int numPartitions = in.readInt();
            Set<TopicUriPartitionAndAssignmentStatus> partitions = new HashSet<>(numPartitions);

            for (int i = 0; i < numPartitions; i++) {
                final String topicUriString = in.readUTF();
                final int partition = in.readInt();
                final int statusCode = in.readInt();
                partitions.add(
                        new TopicUriPartitionAndAssignmentStatus(
                                new TopicUriPartition(topicUriString, partition),
                                AssignmentStatus.ofStatusCode(statusCode)));
            }
            final boolean initialDiscoveryFinished = in.readBoolean();
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized topic partitions");
            }

            return new PscSourceEnumState(partitions, initialDiscoveryFinished);
        }
    }

    private static byte[] serializeTopicPartitions(Collection<TopicUriPartition> topicUriPartitions)
            throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(topicUriPartitions.size());
            for (TopicUriPartition tup : topicUriPartitions) {
                out.writeUTF(tup.getTopicUriAsString());
                out.writeInt(tup.getPartition());
            }
            out.flush();

            return baos.toByteArray();
        }
    }
}
