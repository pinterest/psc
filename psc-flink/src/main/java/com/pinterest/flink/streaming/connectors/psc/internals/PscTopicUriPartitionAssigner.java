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

import org.apache.flink.annotation.Internal;

/**
 * Utility for assigning PSC topic URI partitions to consumer subtasks.
 */
@Internal
public class PscTopicUriPartitionAssigner {

    /**
     * Returns the index of the target subtask that a specific PSC topic URI partition should be
     * assigned to.
     *
     * <p>The resulting distribution of partitions of a single topic has the following contract:
     * <ul>
     *     <li>1. Uniformly distributed across subtasks</li>
     *     <li>2. Partitions are round-robin distributed (strictly clockwise w.r.t. ascending
     *     subtask indices) by using the partition id as the offset from a starting index
     *     (i.e., the index of the subtask which partition 0 of the topic will be assigned to,
     *     determined using the topic name).</li>
     * </ul>
     *
     * <p>The above contract is crucial and cannot be broken. Consumer subtasks rely on this
     * contract to locally filter out partitions that it should not subscribe to, guaranteeing
     * that all partitions of a single topic will always be assigned to some subtask in a
     * uniformly distributed manner.
     *
     * @param partition           the PSC topic URI partition
     * @param numParallelSubtasks total number of parallel subtasks
     * @return index of the target subtask that the PSC topic URI partition should be assigned to.
     */
    public static int assign(PscTopicUriPartition partition, int numParallelSubtasks) {
        // note: this hash is different from flink-kafka-connector in that the topicUri string is used to calculate
        // instead of the topic name itself
        int startIndex = ((partition.getTopicUriStr().hashCode() * 31) & 0x7FFFFFFF) % numParallelSubtasks;

        // here, the assumption is that the id of PSC topic URI partitions are always ascending
        // starting from 0, and therefore can be used directly as the offset clockwise from the start index
        return (startIndex + partition.getPartition()) % numParallelSubtasks;
    }

}
