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

package com.pinterest.flink.connector.psc.dynamic.source.enumerator;

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumState;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumStateSerializer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** (De)serializer for {@link DynamicPscSourceEnumState}. */
@Internal
public class DynamicPscSourceEnumStateSerializer
        implements SimpleVersionedSerializer<DynamicPscSourceEnumState> {

    private static final int VERSION_1 = 1;

    private final PscSourceEnumStateSerializer pscSourceEnumStateSerializer;

    public DynamicPscSourceEnumStateSerializer() {
        this.pscSourceEnumStateSerializer = new PscSourceEnumStateSerializer();
    }

    @Override
    public int getVersion() {
        return VERSION_1;
    }

    @Override
    public byte[] serialize(DynamicPscSourceEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            Set<PscStream> pscStreams = state.getPscStreams();
            serialize(pscStreams, out);

            Map<String, PscSourceEnumState> clusterEnumeratorStates =
                    state.getClusterEnumeratorStates();
            out.writeInt(pscSourceEnumStateSerializer.getVersion());

            // write sub enumerator states
            out.writeInt(clusterEnumeratorStates.size());
            for (Map.Entry<String, PscSourceEnumState> clusterEnumeratorState :
                    clusterEnumeratorStates.entrySet()) {
                String kafkaClusterId = clusterEnumeratorState.getKey();
                out.writeUTF(kafkaClusterId);
                byte[] bytes =
                        pscSourceEnumStateSerializer.serialize(clusterEnumeratorState.getValue());
                // we need to know the exact size of the byte array since
                // KafkaSourceEnumStateSerializer
                // will throw exception if there are leftover unread bytes in deserialization.
                out.writeInt(bytes.length);
                out.write(bytes);
            }

            return baos.toByteArray();
        }
    }

    @Override
    public DynamicPscSourceEnumState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version == VERSION_1) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                Set<PscStream> pscStreams = deserialize(in);

                Map<String, PscSourceEnumState> clusterEnumeratorStates = new HashMap<>();
                int kafkaSourceEnumStateSerializerVersion = in.readInt();

                int clusterEnumeratorStateMapSize = in.readInt();
                for (int i = 0; i < clusterEnumeratorStateMapSize; i++) {
                    String kafkaClusterId = in.readUTF();
                    int byteArraySize = in.readInt();
                    PscSourceEnumState pscSourceEnumState =
                            pscSourceEnumStateSerializer.deserialize(
                                    kafkaSourceEnumStateSerializerVersion,
                                    readNBytes(in, byteArraySize));
                    clusterEnumeratorStates.put(kafkaClusterId, pscSourceEnumState);
                }

                return new DynamicPscSourceEnumState(pscStreams, clusterEnumeratorStates);
            }
        }

        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version up to %d",
                        version, getVersion()));
    }

    private void serialize(Set<KafkaStream> kafkaStreams, DataOutputStream out) throws IOException {
        out.writeInt(kafkaStreams.size());
        for (KafkaStream kafkaStream : kafkaStreams) {
            out.writeUTF(kafkaStream.getStreamId());
            Map<String, ClusterMetadata> clusterMetadataMap = kafkaStream.getClusterMetadataMap();
            out.writeInt(clusterMetadataMap.size());
            for (Map.Entry<String, ClusterMetadata> entry : clusterMetadataMap.entrySet()) {
                String kafkaClusterId = entry.getKey();
                ClusterMetadata clusterMetadata = entry.getValue();
                out.writeUTF(kafkaClusterId);
                out.writeInt(clusterMetadata.getTopicUris().size());
                for (String topic : clusterMetadata.getTopicUris()) {
                    out.writeUTF(topic);
                }

                // only write bootstrap server for now, can extend later to serialize the complete
                // properties
                out.writeUTF(
                        Preconditions.checkNotNull(
                                clusterMetadata
                                        .getProperties()
                                        .getProperty(
                                                PscFlinkConfiguration.CLUSTER_URI_CONFIG,
                                                "cluster.uri must be specified in properties")));
            }
        }
    }

    private Set<PscStream> deserialize(DataInputStream in) throws IOException {

        Set<PscStream> kafkaStreams = new HashSet<>();
        int numStreams = in.readInt();
        for (int i = 0; i < numStreams; i++) {
            String streamId = in.readUTF();
            Map<String, ClusterMetadata> clusterMetadataMap = new HashMap<>();
            int clusterMetadataMapSize = in.readInt();
            for (int j = 0; j < clusterMetadataMapSize; j++) {
                String clusterId = in.readUTF();
                int topicsSize = in.readInt();
                Set<String> topics = new HashSet<>();
                for (int k = 0; k < topicsSize; k++) {
                    topics.add(in.readUTF());
                }

                String clusterUri = in.readUTF();
                Properties properties = new Properties();
                properties.setProperty(
                        PscFlinkConfiguration.CLUSTER_URI_CONFIG, clusterUri);

                clusterMetadataMap.put(clusterId, new ClusterMetadata(topics, properties));
            }

            kafkaStreams.add(new PscStream(streamId, clusterMetadataMap));
        }

        return kafkaStreams;
    }

    private static byte[] readNBytes(DataInputStream in, int size) throws IOException {
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return bytes;
    }
}
