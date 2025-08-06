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

import com.pinterest.psc.common.TopicUriPartition;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** A {@link SourceSplit} for a PSC topicUriPartition. */
@Internal
public class PscTopicUriPartitionSplit implements SourceSplit {
    public static final long NO_STOPPING_OFFSET = Long.MIN_VALUE;
    // Indicating the split should consume from the latest.
    public static final long LATEST_OFFSET = -1;
    // Indicating the split should consume from the earliest.
    public static final long EARLIEST_OFFSET = -2;
    // Indicating the split should consume from the last committed offset.
    public static final long COMMITTED_OFFSET = -3;

    // Valid special starting offsets
    public static final Set<Long> VALID_STARTING_OFFSET_MARKERS =
            new HashSet<>(Arrays.asList(EARLIEST_OFFSET, LATEST_OFFSET, COMMITTED_OFFSET));
    public static final Set<Long> VALID_STOPPING_OFFSET_MARKERS =
            new HashSet<>(Arrays.asList(LATEST_OFFSET, COMMITTED_OFFSET, NO_STOPPING_OFFSET));

    private final TopicUriPartition tup;
    private final long startingOffset;
    private final long stoppingOffset;

    public PscTopicUriPartitionSplit(TopicUriPartition tup, long startingOffset) {
        this(tup, startingOffset, NO_STOPPING_OFFSET);
    }

    public PscTopicUriPartitionSplit(TopicUriPartition tup, long startingOffset, long stoppingOffset) {
        verifyInitialOffset(tup, startingOffset, stoppingOffset);
        this.tup = tup;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
    }

    public String getTopicUri() {
        return tup.getTopicUriAsString();
    }

    public int getPartition() {
        return tup.getPartition();
    }

    public TopicUriPartition getTopicUriPartition() {
        return tup;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset > 0
                        || stoppingOffset == LATEST_OFFSET
                        || stoppingOffset == COMMITTED_OFFSET
                ? Optional.of(stoppingOffset)
                : Optional.empty();
    }

    @Override
    public String splitId() {
        return toSplitId(tup);
    }

    @Override
    public String toString() {
        return String.format(
                "[Partition: %s, StartingOffset: %d, StoppingOffset: %d]",
                tup, startingOffset, stoppingOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tup, startingOffset, stoppingOffset);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PscTopicUriPartitionSplit)) {
            return false;
        }
        PscTopicUriPartitionSplit other = (PscTopicUriPartitionSplit) obj;
        return tup.equals(other.tup)
                && startingOffset == other.startingOffset
                && stoppingOffset == other.stoppingOffset;
    }

    public static String toSplitId(TopicUriPartition tup) {
        return tup.toString();
    }

    // ------------ private methods ---------------

    private static void verifyInitialOffset(
            TopicUriPartition tp, Long startingOffset, long stoppingOffset) {
        if (startingOffset == null) {
            throw new FlinkRuntimeException(
                    "Cannot initialize starting offset for partition " + tp);
        }

        if (startingOffset < 0 && !VALID_STARTING_OFFSET_MARKERS.contains(startingOffset)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Invalid starting offset %d is specified for partition %s. "
                                    + "It should either be non-negative or be one of the "
                                    + "[%d(earliest), %d(latest), %d(committed)].",
                            startingOffset, tp, LATEST_OFFSET, EARLIEST_OFFSET, COMMITTED_OFFSET));
        }

        if (stoppingOffset < 0 && !VALID_STOPPING_OFFSET_MARKERS.contains(stoppingOffset)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Illegal stopping offset %d is specified for partition %s. "
                                    + "It should either be non-negative or be one of the "
                                    + "[%d(latest), %d(committed), %d(Long.MIN_VALUE, no_stopping_offset)].",
                            stoppingOffset,
                            tp,
                            LATEST_OFFSET,
                            COMMITTED_OFFSET,
                            NO_STOPPING_OFFSET));
        }
    }
}
