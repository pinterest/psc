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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(PscTopicUriPartitionSplitSerializer.class);

    /**
     * We set 100,000 as the base version for PSC serializer. This is set to a large number to avoid conflicts with
     * native Flink-Kafka serializer versions which start from 0. DO NOT CHANGE THIS VALUE.
     */
    private static final int BASE_PSC_VERSION = 100_000;

    /**
     * The current version for PSC serializer. This can be incremented by 1 every time the
     * serialization format changes.
     */
    private static final int CURRENT_VERSION = BASE_PSC_VERSION;
    private String clusterUri = null;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    public void setClusterUri(String clusterUri) {
        LOG.info("Setting cluster URI: " + clusterUri);
        this.clusterUri = clusterUri;
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
            LOG.info("Serializing split with version: " + getVersion() + " for topicUri-partition: " + split.getTopicUri() + "-" + split.getPartition());
            return baos.toByteArray();
        }
    }

    @Override
    public PscTopicUriPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        LOG.info("Deserializing split with version: " + version);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            if (!isPscVersion(version)) {
                LOG.info("Detected non-PSC version in serialized split. Deserializing as Flink-Kafka split.");
                return deserializeFromFlinkKafkaCheckpoint(in);
            }
            LOG.info("Detected PSC version in serialized split. Deserializing as PSC split.");
            String topicUri = in.readUTF();
            int partition = in.readInt();
            long offset = in.readLong();
            long stoppingOffset = in.readLong();
            PscTopicUriPartitionSplit result = new PscTopicUriPartitionSplit(
                    new TopicUriPartition(topicUri, partition), offset, stoppingOffset);
            LOG.info("Deserialized split: " + result);
            return result;
        }
    }

    private boolean isPscVersion(int version) {
        return version >= BASE_PSC_VERSION;
    }

    private PscTopicUriPartitionSplit deserializeFromFlinkKafkaCheckpoint(DataInputStream in) throws IOException {
        String topic = in.readUTF();
        int partition = in.readInt();
        long offset = in.readLong();
        long stoppingOffset = in.readLong();

        // we are likely reading from a Flink-Kafka checkpoint here. To support recovering from Flink-Kafka
        // checkpoints, we will assume that topicUri here is actually just the topic name, and prepend the
        // cluster URI to it.
        if (clusterUri == null) {
            throw new IllegalStateException("Cluster URI not set. Cannot deserialize split.");
        }
        String topicUri = clusterUri + topic;
        LOG.info("Prepended cluster URI to deserialized topicName {} so that topicUri is now: {}", topic, topicUri);
        return new PscTopicUriPartitionSplit(
                new TopicUriPartition(topicUri, partition), offset, stoppingOffset);
    }
}
