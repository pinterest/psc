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

package com.pinterest.flink.connector.psc.source.enumerator.initializer;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} to initialize the offsets based on a timestamp.
 * If the message meeting the requirement of the timestamp have not been produced yet, just
 * use the latest offset.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
class TimestampOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 2932230571773627233L;
    private final long startingTimestamp;

    TimestampOffsetsInitializer(long startingTimestamp) {
        this.startingTimestamp = startingTimestamp;
    }

    @Override
    public Map<TopicUriPartition, Long> getPartitionOffsets(
            Collection<TopicUriPartition> partitions,
            PartitionOffsetsRetriever partitionOffsetsRetriever) {
        Map<TopicUriPartition, Long> startingTimestamps = new HashMap<>();
        Map<TopicUriPartition, Long> initialOffsets = new HashMap<>();

        // First get the current end offsets of the partitions. This is going to be used
        // in case we cannot find a suitable offsets based on the timestamp, i.e. the message
        // meeting the requirement of the timestamp have not been produced to PSC yet, in
        // this case, we just use the latest offset.
        // We need to get the latest offsets before querying offsets by time to ensure that
        // no message is going to be missed.
        Map<TopicUriPartition, Long> endOffsets = partitionOffsetsRetriever.endOffsets(partitions);
        partitions.forEach(tp -> startingTimestamps.put(tp, startingTimestamp));
        Map<TopicUriPartition, Long> topicPartitionOffsetMap =
                partitionOffsetsRetriever.offsetsForTimes(startingTimestamps);

        for (TopicUriPartition tp : partitions) {
            // offset may not have been resolved
            if (topicPartitionOffsetMap.containsKey(tp)) {
                initialOffsets.put(tp, topicPartitionOffsetMap.get(tp));
            } else {
                initialOffsets.put(tp, endOffsets.get(tp));
            }
        }
        return initialOffsets;
    }

    @Override
    public String getAutoOffsetResetStrategy() {
        return PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST;
    }
}
