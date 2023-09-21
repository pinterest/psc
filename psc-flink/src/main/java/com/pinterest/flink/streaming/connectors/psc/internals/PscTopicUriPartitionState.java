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
 * The state that the Flink PSC Consumer holds for each PSC topic URI partition.
 * Includes the PSC descriptor for partitions.
 *
 * <p>This class describes the most basic state (only the offset), subclasses
 * define more elaborate state, containing current watermarks and timestamp
 * extractors.
 *
 * @param <TUPH> The type of the PSC topic URI partition descriptor, which varies across PSC versions.
 */
@Internal
public class PscTopicUriPartitionState<T, TUPH> {

    // ------------------------------------------------------------------------

    /**
     * The Flink description of a PSC topic URI partition.
     */
    private final PscTopicUriPartition pscTopicUriPartition;

    /**
     * The PSC description of a PSC topic URI partition (varies across different PSC versions).
     */
    private final TUPH pscTopicUriPartitionHandle;

    /**
     * The offset within the PSC topic URI partition that we already processed.
     */
    private volatile long offset;

    /**
     * The offset of the PSC topic URI partition that has been committed.
     */
    private volatile long committedOffset;

    // ------------------------------------------------------------------------

    public PscTopicUriPartitionState(PscTopicUriPartition pscTopicUriPartition, TUPH pscTopicUriPartitionHandle) {
        this.pscTopicUriPartition = pscTopicUriPartition;
        this.pscTopicUriPartitionHandle = pscTopicUriPartitionHandle;
        this.offset = PscTopicUriPartitionStateSentinel.OFFSET_NOT_SET;
        this.committedOffset = PscTopicUriPartitionStateSentinel.OFFSET_NOT_SET;
    }

    // ------------------------------------------------------------------------

    /**
     * Gets Flink's descriptor for the PSC topic URI Partition.
     *
     * @return The Flink partition descriptor.
     */
    public final PscTopicUriPartition getPscTopicUriPartition() {
        return pscTopicUriPartition;
    }

    /**
     * Gets PSC's descriptor for the PSC topic URI Partition.
     *
     * @return The PSC topic URI partition descriptor.
     */
    public final TUPH getPscTopicUriPartitionHandle() {
        return pscTopicUriPartitionHandle;
    }

    public final String getTopicUri() {
        return pscTopicUriPartition.getTopicUriStr();
    }

    /**
     * The current offset in the partition. This refers to the offset last element that
     * we retrieved and emitted successfully. It is the offset that should be stored in
     * a checkpoint.
     */
    public final long getOffset() {
        return offset;
    }

    public final void setOffset(long offset) {
        this.offset = offset;
    }

    public final boolean isOffsetDefined() {
        return offset != PscTopicUriPartitionStateSentinel.OFFSET_NOT_SET;
    }

    public final void setCommittedOffset(long offset) {
        this.committedOffset = offset;
    }

    public final long getCommittedOffset() {
        return committedOffset;
    }

    public long extractTimestamp(T record, long pubsubEventTimestamp) {
        return pubsubEventTimestamp;
    }

    public void onEvent(T event, long timestamp) {
        // do nothing
    }

    public void onPeriodicEmit() {
        // do nothing
    }


    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "Partition: " + pscTopicUriPartition + ", PscTopicUriPartitionHandle=" + pscTopicUriPartitionHandle
                + ", offset=" + (isOffsetDefined() ? String.valueOf(offset) : "(not set)");
    }
}
