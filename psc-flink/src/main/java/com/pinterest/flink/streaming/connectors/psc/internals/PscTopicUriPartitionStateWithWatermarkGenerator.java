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
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * A special version of the per-PSC-partition-state that additionally holds a {@link
 * TimestampAssigner}, {@link WatermarkGenerator}, an immediate {@link WatermarkOutput}, and a
 * deferred {@link WatermarkOutput} for this partition.
 *
 * <p>See {@link org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer} for an
 * explanation
 * of immediate and deferred {@link WatermarkOutput WatermarkOutputs.}.
 *
 * @param <T>    The type of records handled by the watermark generator
 * @param <TUPH> The type of the PSC topic URI partition descriptor, which varies across PSC versions.
 */
@Internal
public final class PscTopicUriPartitionStateWithWatermarkGenerator<T, TUPH> extends PscTopicUriPartitionState<T, TUPH> {

    private final TimestampAssigner<T> timestampAssigner;

    private final WatermarkGenerator<T> watermarkGenerator;

    /**
     * Refer to {@link org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer} for
     * a description of immediate/deferred output.
     */
    private final WatermarkOutput immediateOutput;

    /**
     * Refer to {@link org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer} for
     * a description of immediate/deferred output.
     */
    private final WatermarkOutput deferredOutput;

    // ------------------------------------------------------------------------

    public PscTopicUriPartitionStateWithWatermarkGenerator(
            PscTopicUriPartition pscTopicUriPartition,
            TUPH pscTopicUriPartitionHandle,
            TimestampAssigner<T> timestampAssigner,
            WatermarkGenerator<T> watermarkGenerator,
            WatermarkOutput immediateOutput,
            WatermarkOutput deferredOutput) {
        super(pscTopicUriPartition, pscTopicUriPartitionHandle);

        this.timestampAssigner = timestampAssigner;
        this.watermarkGenerator = watermarkGenerator;
        this.immediateOutput = immediateOutput;
        this.deferredOutput = deferredOutput;
    }

    // ------------------------------------------------------------------------

    @Override
    public long extractTimestamp(T record, long pubsubEventTimestamp) {
        return timestampAssigner.extractTimestamp(record, pubsubEventTimestamp);
    }

    @Override
    public void onEvent(T event, long timestamp) {
        watermarkGenerator.onEvent(event, timestamp, immediateOutput);
    }

    @Override
    public void onPeriodicEmit() {
        watermarkGenerator.onPeriodicEmit(deferredOutput);
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "PscTopicUriPartitionStateWithPeriodicWatermarks: partition=" + getPscTopicUriPartition()
                + ", offset=" + getOffset();
    }
}
