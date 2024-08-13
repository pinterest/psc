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

package com.pinterest.flink.connector.psc.source.split;

import com.pinterest.psc.common.TopicUriPartition;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The {@link SimpleVersionedSerializer serializer} for {@link
 * PscTopicUriPartitionSplit}.
 */
@Internal
public class PscTopicUriPartitionSplitSerializer
        implements SimpleVersionedSerializer<PscTopicUriPartitionSplit> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PscTopicUriPartitionSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.getTopicUri());
            out.writeInt(split.getPartition());
            out.writeLong(split.getStartingOffset());
            out.writeLong(split.getStoppingOffset().orElse(PscTopicUriPartitionSplit.NO_STOPPING_OFFSET));
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PscTopicUriPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String topicUri = in.readUTF();
            int partition = in.readInt();
            long offset = in.readLong();
            long stoppingOffset = in.readLong();
            return new PscTopicUriPartitionSplit(
                    new TopicUriPartition(topicUri, partition), offset, stoppingOffset);
        }
    }
}
