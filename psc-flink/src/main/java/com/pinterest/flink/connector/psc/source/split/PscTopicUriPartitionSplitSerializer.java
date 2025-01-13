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

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
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
    private static final int CURRENT_VERSION = 0;
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
        Thread.dumpStack();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String topicUri = in.readUTF();
            try {
                // try to validate topicUri
                BaseTopicUri.validate(topicUri);
            } catch (TopicUriSyntaxException e) {
                LOG.info("Detected a possible Flink-Kafka checkpoint with topic: " + topicUri);
                // we are likely reading from a Flink-Kafka checkpoint here. To support recovering from Flink-Kafka
                // checkpoints, we will assume that topicUri here is actually just the topic name, and prepend the
                // cluster URI to it.
                if (clusterUri == null) {
                    throw new IllegalStateException("Cluster URI not set. Cannot deserialize split.");
                }
                topicUri = clusterUri + topicUri;
                LOG.info("Prepending cluster URI to topic so that topicUri is now: " + topicUri);
            }
            int partition = in.readInt();
            long offset = in.readLong();
            long stoppingOffset = in.readLong();
            LOG.info("Deserialized split for topicUri-partition: " + topicUri + "-" + partition + " for offset=" + offset + " and stopping offset=" + stoppingOffset);
            return new PscTopicUriPartitionSplit(
                    new TopicUriPartition(topicUri, partition), offset, stoppingOffset);
        }
    }
}
