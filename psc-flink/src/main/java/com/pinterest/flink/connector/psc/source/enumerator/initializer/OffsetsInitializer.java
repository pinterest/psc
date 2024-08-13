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
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * An interface for users to specify the starting / stopping offset of a {@link
 * PscTopicUriPartitionSplit}.
 *
 * @see ReaderHandledOffsetsInitializer
 * @see SpecifiedOffsetsInitializer
 * @see TimestampOffsetsInitializer
 */
@PublicEvolving
public interface OffsetsInitializer extends Serializable {

    /**
     * Get the initial offsets for the given Kafka partitions. These offsets will be used as either
     * starting offsets or stopping offsets of the Kafka partitions.
     *
     * <p>If the implementation returns a starting offset which causes {@code
     * OffsetsOutOfRangeException} from Kafka. The offsetResetStrategy provided by the
     * {@link #getAutoOffsetResetStrategy()} will be used to reset the offset.
     *
     * @param partitions the Kafka partitions to get the starting offsets.
     * @param partitionOffsetsRetriever a helper to retrieve information of the Kafka partitions.
     * @return A mapping from Kafka partition to their offsets to start consuming from.
     */
    Map<TopicUriPartition, Long> getPartitionOffsets(
            Collection<TopicUriPartition> partitions,
            PartitionOffsetsRetriever partitionOffsetsRetriever);

    /**
     * Get the auto offset reset strategy in case the initialized offsets falls out of the range.
     *
     * <p>The OffsetStrategy is only used when the offset initializer is used to initialize the
     * starting offsets and the starting offsets is out of range.
     *
     * @return An offsetResetStrategy to use if the initialized offsets are out of the
     *     range.
     */
    String getAutoOffsetResetStrategy();

    /**
     * An interface that provides necessary information to the {@link OffsetsInitializer} to get the
     * initial offsets of the Kafka partitions.
     */
    interface PartitionOffsetsRetriever {

        /**
         * The group id should be the set for {@link com.pinterest.flink.connector.psc.source.PscSource PscSource} before invoking this
         * method. Otherwise an {@code IllegalStateException} will be thrown.
         *
         * @throws IllegalStateException if the group id is not set for the {@code KafkaSource}.
         */
        Map<TopicUriPartition, Long> committedOffsets(Collection<TopicUriPartition> partitions);

        /** List end offsets for the specified partitions. */
        Map<TopicUriPartition, Long> endOffsets(Collection<TopicUriPartition> partitions);

        /** List beginning offsets for the specified partitions. */
        Map<TopicUriPartition, Long> beginningOffsets(Collection<TopicUriPartition> partitions);

        /** List offsets matching a timestamp for the specified partitions. */
        Map<TopicUriPartition, MessageId> offsetsForTimes(
                Map<TopicUriPartition, Long> timestampsToSearch);
    }

    // --------------- factory methods ---------------

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the committed offsets. An
     * exception will be thrown at runtime if there is no committed offsets.
     *
     * @return an offset initializer which initialize the offsets to the committed offsets.
     */
    static OffsetsInitializer committedOffsets() {
        return committedOffsets(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the committed offsets. Use
     * the given offsetResetStrategy to initialize the offsets if the committed offsets does
     * not exist.
     *
     * @param offsetResetStrategy the offset reset strategy to use when the committed offsets do not
     *     exist.
     * @return an {@link OffsetsInitializer} which initializes the offsets to the committed offsets.
     */
    static OffsetsInitializer committedOffsets(String offsetResetStrategy) {
        return new ReaderHandledOffsetsInitializer(
                PscTopicUriPartitionSplit.COMMITTED_OFFSET, offsetResetStrategy);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets in each partition so that the
     * initialized offset is the offset of the first record whose record timestamp is greater than
     * or equals the give timestamp (milliseconds).
     *
     * @param timestamp the timestamp (milliseconds) to start the consumption.
     * @return an {@link OffsetsInitializer} which initializes the offsets based on the given
     *     timestamp.
     */
    static OffsetsInitializer timestamp(long timestamp) {
        return new TimestampOffsetsInitializer(timestamp);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     * offsets of each partition.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     *     offsets.
     */
    static OffsetsInitializer earliest() {
        return new ReaderHandledOffsetsInitializer(
                PscTopicUriPartitionSplit.EARLIEST_OFFSET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the latest offsets of each
     * partition.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the latest offsets.
     */
    static OffsetsInitializer latest() {
        return new ReaderHandledOffsetsInitializer(
                PscTopicUriPartitionSplit.LATEST_OFFSET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the specified offsets.
     *
     * @param offsets the specified offsets for each partition.
     * @return an {@link OffsetsInitializer} which initializes the offsets to the specified offsets.
     */
    static OffsetsInitializer offsets(Map<TopicUriPartition, Long> offsets) {
        return new SpecifiedOffsetsInitializer(offsets, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the specified offsets. Use
     * the given offsetResetStrategy to initialize the offsets in case the specified offset
     * is out of range.
     *
     * @param offsets the specified offsets for each partition.
     * @param offsetResetStrategy the offsetResetStrategy to use when the specified offset
     *     is out of range.
     * @return an {@link OffsetsInitializer} which initializes the offsets to the specified offsets.
     */
    static OffsetsInitializer offsets(
            Map<TopicUriPartition, Long> offsets, String offsetResetStrategy) {
        return new SpecifiedOffsetsInitializer(offsets, offsetResetStrategy);
    }
}
