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

import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumState;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumStateSerializer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

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

    /**
     * We set 100,000 as the base version for PSC serializer. This is set to a large number to avoid
     * conflicts with native Flink-Kafka serializer versions which start from 0. DO NOT CHANGE THIS VALUE.
     */
    private static final int BASE_PSC_VERSION = 100_000;

    /**
     * The current version for PSC serializer. This can be incremented by 1 every time the
     * serialization format changes.
     */
    private static final int CURRENT_VERSION = BASE_PSC_VERSION;

    private final PscSourceEnumStateSerializer pscSourceEnumStateSerializer;

    public DynamicPscSourceEnumStateSerializer() {
        this.pscSourceEnumStateSerializer = new PscSourceEnumStateSerializer();
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
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
                String clusterId = clusterEnumeratorState.getKey();
                out.writeUTF(clusterId);
                byte[] bytes =
                        pscSourceEnumStateSerializer.serialize(clusterEnumeratorState.getValue());
                // we need to know the exact size of the byte array since
                // PscSourceEnumStateSerializer
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
        if (version == CURRENT_VERSION) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                Set<PscStream> pscStreams = deserialize(in);

                Map<String, PscSourceEnumState> clusterEnumeratorStates = new HashMap<>();
                int pscSourceEnumStateSerializerVersion = in.readInt();

                int clusterEnumeratorStateMapSize = in.readInt();
                for (int i = 0; i < clusterEnumeratorStateMapSize; i++) {
                    String clusterId = in.readUTF();
                    int byteArraySize = in.readInt();
                    PscSourceEnumState pscSourceEnumState =
                            pscSourceEnumStateSerializer.deserialize(
                                    pscSourceEnumStateSerializerVersion,
                                    readNBytes(in, byteArraySize));
                    clusterEnumeratorStates.put(clusterId, pscSourceEnumState);
                }

                return new DynamicPscSourceEnumState(pscStreams, clusterEnumeratorStates);
            }
        }

        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version %d",
                        version, getVersion()));
    }

    private void serialize(Set<PscStream> pscStreams, DataOutputStream out) throws IOException {
        out.writeInt(pscStreams.size());
        for (PscStream pscStream : pscStreams) {
            out.writeUTF(pscStream.getStreamId());
            Map<String, ClusterMetadata> clusterMetadataMap = pscStream.getClusterMetadataMap();
            out.writeInt(clusterMetadataMap.size());
            for (Map.Entry<String, ClusterMetadata> entry : clusterMetadataMap.entrySet()) {
                String clusterId = entry.getKey();
                ClusterMetadata clusterMetadata = entry.getValue();
                out.writeUTF(clusterId);
                out.writeInt(clusterMetadata.getTopics().size());
                for (String topic : clusterMetadata.getTopics()) {
                    out.writeUTF(topic);
                }
                serializeProperties(clusterMetadata.getProperties(), out);
            }
        }
    }

    private Set<PscStream> deserialize(DataInputStream in) throws IOException {

        Set<PscStream> pscStreams = new HashSet<>();
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

                Properties properties = new Properties();
                deserializeProperties(properties, in);

                clusterMetadataMap.put(clusterId, new ClusterMetadata(topics, properties));
            }

            pscStreams.add(new PscStream(streamId, clusterMetadataMap));
        }

        return pscStreams;
    }

    private void serializeProperties(Properties properties, DataOutputStream out) throws IOException {
        out.writeInt(properties.size());
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            out.writeUTF(entry.getKey().toString());
            out.writeUTF(entry.getValue().toString());
        }
    }

    private void deserializeProperties(Properties properties, DataInputStream in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            properties.setProperty(in.readUTF(), in.readUTF());
        }
    }

    private static byte[] readNBytes(DataInputStream in, int size) throws IOException {
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return bytes;
    }
}
