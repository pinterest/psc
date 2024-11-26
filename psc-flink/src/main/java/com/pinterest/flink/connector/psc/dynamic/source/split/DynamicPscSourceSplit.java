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

package com.pinterest.flink.connector.psc.dynamic.source.split;

import com.google.common.base.MoreObjects;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** Split that wraps {@link KafkaPartitionSplit} with Kafka cluster information. */
@Internal
public class DynamicPscSourceSplit extends PscTopicUriPartitionSplit {

    private final String clusterId;
    private final PscTopicUriPartitionSplit pscTopicUriPartitionSplit;

    public DynamicPscSourceSplit(String clusterId, PscTopicUriPartitionSplit pscTopicUriPartitionSplit) {
        super(
                pscTopicUriPartitionSplit.getTopicUriPartition(),
                pscTopicUriPartitionSplit.getStartingOffset(),
                pscTopicUriPartitionSplit.getStoppingOffset().orElse(NO_STOPPING_OFFSET));
        this.clusterId = clusterId;
        this.pscTopicUriPartitionSplit = pscTopicUriPartitionSplit;
    }

    @Override
    public String splitId() {
        return clusterId + "-" + pscTopicUriPartitionSplit.splitId();
    }

    public String getClusterId() {
        return clusterId;
    }

    public PscTopicUriPartitionSplit getPscTopicUriPartitionSplit() {
        return pscTopicUriPartitionSplit;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("kafkaClusterId", clusterId)
                .add("kafkaPartitionSplit", pscTopicUriPartitionSplit)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DynamicPscSourceSplit that = (DynamicPscSourceSplit) o;
        return Objects.equals(clusterId, that.clusterId)
                && Objects.equals(pscTopicUriPartitionSplit, that.pscTopicUriPartitionSplit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), clusterId, pscTopicUriPartitionSplit);
    }
}
