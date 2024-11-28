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

import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of {@link org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer} which initializes the offsets of the partition
 * according to the user specified offsets.
 *
 * <p>Use specified offsets for specified partitions while use commit offsets or offsetResetStrategy
 * for unspecified partitions. Specified partition's offset should be less than its latest offset,
 * otherwise it will start from the offsetResetStrategy. The default value of offsetResetStrategy is
 * earliest.
 *
 * <p>Package private and should be instantiated via {@link org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer}.
 */
class SpecifiedOffsetsInitializer implements OffsetsInitializer, OffsetsInitializerValidator {
    private static final long serialVersionUID = 1649702397250402877L;
    private final Map<TopicUriPartition, Long> initialOffsets;
    private final String offsetResetStrategy;

    SpecifiedOffsetsInitializer(
            Map<TopicUriPartition, Long> initialOffsets, String offsetResetStrategy) {
        this.initialOffsets = Collections.unmodifiableMap(initialOffsets);
        this.offsetResetStrategy = offsetResetStrategy;
    }

    @Override
    public Map<TopicUriPartition, Long> getPartitionOffsets(
            Collection<TopicUriPartition> partitions,
            PartitionOffsetsRetriever partitionOffsetsRetriever) {
        Map<TopicUriPartition, Long> offsets = new HashMap<>();
        List<TopicUriPartition> toLookup = new ArrayList<>();
        for (TopicUriPartition tp : partitions) {
            Long offset = initialOffsets.get(tp);
            if (offset == null) {
                toLookup.add(tp);
            } else {
                offsets.put(tp, offset);
            }
        }
        if (!toLookup.isEmpty()) {
            // First check the committed offsets.
            Map<TopicUriPartition, Long> committedOffsets =
                    partitionOffsetsRetriever.committedOffsets(toLookup);
            offsets.putAll(committedOffsets);
            toLookup.removeAll(committedOffsets.keySet());

            switch (offsetResetStrategy) {
                case (PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST):
                    offsets.putAll(partitionOffsetsRetriever.beginningOffsets(toLookup));
                    break;
                case (PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST):
                    offsets.putAll(partitionOffsetsRetriever.endOffsets(toLookup));
                    break;
                default:
                    throw new IllegalStateException(
                            "Cannot find initial offsets for partitions: " + toLookup);
            }
        }
        return offsets;
    }

    @Override
    public String getAutoOffsetResetStrategy() {
        return offsetResetStrategy;
    }

    @Override
    public void validate(Properties pscSourceProperties) {
        initialOffsets.forEach(
                (tp, offset) -> {
                    if (offset == PscTopicUriPartitionSplit.COMMITTED_OFFSET) {
                        checkState(
                                pscSourceProperties.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID),
                                String.format(
                                        "Property %s is required because partition %s is initialized with committed offset",
                                        PscConfiguration.PSC_CONSUMER_GROUP_ID, tp));
                    }
                });
    }
}
