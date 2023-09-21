/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.psc.common.ByteArrayHandler;
import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Flink's description of a partition in a PSC topic URI.
 * Serializable, and common across all PSC consumer subclasses.
 *
 * <p>Note: This class must not change in its structure, because it would change the
 * serialization format and make previous savepoints unreadable.
 */
@PublicEvolving
public final class PscTopicUriPartition implements Serializable {

    /**
     * THIS SERIAL VERSION UID MUST NOT CHANGE, BECAUSE IT WOULD BREAK
     * READING OLD SERIALIZED INSTANCES FROM SAVEPOINTS.
     */
    private static final long serialVersionUID = 722083576322742325L;

    // ------------------------------------------------------------------------

    private final byte[] serializedTopicUriPartition;
    private final String protocol;
    private final int cachedHash;

    public PscTopicUriPartition(TopicUri topicUri, int partition) {
        this.protocol = topicUri.getProtocol();
        byte[] serializedTopicUri = topicUri.serialize();
        this.serializedTopicUriPartition = ByteArrayHandler.serialize(serializedTopicUri, partition);
        this.cachedHash = calculateHash();
    }

    public PscTopicUriPartition(String topicUriStr, int partition) {
        TopicUri topicUri;
        try {
            topicUri = TopicUri.validate(topicUriStr);
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }
        this.protocol = topicUri.getProtocol();
        byte[] serializedTopicUri = topicUri.serialize();
        this.serializedTopicUriPartition = ByteArrayHandler.serialize(serializedTopicUri, partition);
        this.cachedHash = calculateHash();
    }

    // ------------------------------------------------------------------------

    public String getTopicUriStr() {
        return getTopicUri().getTopicUriAsString();
    }

    public TopicUri getTopicUri() {
        ByteArrayHandler byteArrayHandler = new ByteArrayHandler(serializedTopicUriPartition);
        try {
            return TopicUri.deserialize(byteArrayHandler.readArray(), protocol);
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public int getPartition() {
        ByteArrayHandler byteArrayHandler = new ByteArrayHandler(serializedTopicUriPartition);
        try {
            TopicUri.deserialize(byteArrayHandler.readArray(), protocol);
        } catch (TopicUriSyntaxException e) {
            throw new RuntimeException(e);
        }
        return byteArrayHandler.readInt();
    }

    public byte[] getSerializedTopicUriPartition() {
        return serializedTopicUriPartition;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("PscTopicUriPartition {topicUriStr=%s, partition=%d}",
                getTopicUriStr(), getPartition());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof PscTopicUriPartition) {
            PscTopicUriPartition that = (PscTopicUriPartition) o;
            return PscCommon.equals(this.serializedTopicUriPartition, that.serializedTopicUriPartition) &&
                    PscCommon.equals(this.protocol, that.protocol);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return cachedHash;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private int calculateHash() {
        return 31 * Arrays.hashCode(serializedTopicUriPartition) + protocol.hashCode();
    }

    public static String toString(Map<PscTopicUriPartition, Long> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<PscTopicUriPartition, Long> p : map.entrySet()) {
            PscTopicUriPartition ktp = p.getKey();
            sb.append(ktp.getTopicUriStr()).append(":").append(ktp.getPartition()).append("=").append(p.getValue()).append(", ");
        }
        return sb.toString();
    }

    public static String toString(List<PscTopicUriPartition> partitions) {
        StringBuilder sb = new StringBuilder();
        for (PscTopicUriPartition p : partitions) {
            sb.append(p.getTopicUriStr()).append(":").append(p.getPartition()).append(", ");
        }
        return sb.toString();
    }

    public static List<PscTopicUriPartition> dropLeaderData(List<PscTopicUriPartitionLeader> partitionInfos) {
        List<PscTopicUriPartition> ret = new ArrayList<>(partitionInfos.size());
        for (PscTopicUriPartitionLeader ktpl : partitionInfos) {
            ret.add(ktpl.getPscTopicUriPartition());
        }
        return ret;
    }

    /**
     * A {@link java.util.Comparator} for {@link PscTopicUriPartition}s.
     */
    public static class Comparator implements java.util.Comparator<PscTopicUriPartition> {
        @Override
        public int compare(PscTopicUriPartition p1, PscTopicUriPartition p2) {
            return PscCommon.compare(p1.getSerializedTopicUriPartition(), p2.getSerializedTopicUriPartition());
        }
    }
}
