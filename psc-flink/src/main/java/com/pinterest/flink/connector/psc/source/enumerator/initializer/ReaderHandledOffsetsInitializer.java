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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A initializer that initialize the partitions to the earliest / latest / last-committed offsets.
 * The offsets initialization are taken care of by the {@code PscTopicUriPartitionSplitReader} instead of
 * by the {@code PscSourceEnumerator}.
 *
 * <p>Package private and should be instantiated via {@link org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer}.
 */
class ReaderHandledOffsetsInitializer implements OffsetsInitializer, OffsetsInitializerValidator {
    private static final long serialVersionUID = 172938052008787981L;
    private final long startingOffset;
    private final String offsetResetStrategy;

    /**
     * The only valid value for startingOffset is following. {@link
     * PscTopicUriPartitionSplit#EARLIEST_OFFSET EARLIEST_OFFSET}, {@link
     * PscTopicUriPartitionSplit#LATEST_OFFSET LATEST_OFFSET}, {@link PscTopicUriPartitionSplit#COMMITTED_OFFSET
     * COMMITTED_OFFSET}
     */
    ReaderHandledOffsetsInitializer(long startingOffset, String offsetResetStrategy) {
        this.startingOffset = startingOffset;
        this.offsetResetStrategy = offsetResetStrategy;
    }

    @Override
    public Map<TopicUriPartition, Long> getPartitionOffsets(
            Collection<TopicUriPartition> partitions,
            PartitionOffsetsRetriever partitionOffsetsRetriever) {
        Map<TopicUriPartition, Long> initialOffsets = new HashMap<>();
        for (TopicUriPartition tp : partitions) {
            initialOffsets.put(tp, startingOffset);
        }
        return initialOffsets;
    }

    @Override
    public String getAutoOffsetResetStrategy() {
        return offsetResetStrategy;
    }

    @Override
    public void validate(Properties kafkaSourceProperties) {
        if (startingOffset == PscTopicUriPartitionSplit.COMMITTED_OFFSET) {
            checkState(
                    kafkaSourceProperties.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID),
                    String.format(
                            "Property %s is required when using committed offset for offsets initializer",
                            PscConfiguration.PSC_CONSUMER_GROUP_ID));
        }
    }
}
