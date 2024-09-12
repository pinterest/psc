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

    private static final int VERSION_0 = 0;
    private static final int VERSION_1 = 1;

    private static final int CURRENT_VERSION = VERSION_1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PscSourceEnumState enumState) throws IOException {
        return serializeTopicPartitions(enumState.assignedPartitions());
    }

    @Override
    public PscSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version == CURRENT_VERSION) {
            final Set<TopicUriPartition> assignedPartitions = deserializeTopicPartitions(serialized);
            return new PscSourceEnumState(assignedPartitions);
        }

        // Backward compatibility
        if (version == VERSION_0) {
            Map<Integer, Set<PscTopicUriPartitionSplit>> currentPartitionAssignment =
                    SerdeUtils.deserializeSplitAssignments(
                            serialized, new PscTopicUriPartitionSplitSerializer(), HashSet::new);
            Set<TopicUriPartition> currentAssignedSplits = new HashSet<>();
            currentPartitionAssignment.forEach(
                    (reader, splits) ->
                            splits.forEach(
                                    split -> currentAssignedSplits.add(split.getTopicUriPartition())));
            return new PscSourceEnumState(currentAssignedSplits);
        }

        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version up to %d",
                        version, CURRENT_VERSION));
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
}
