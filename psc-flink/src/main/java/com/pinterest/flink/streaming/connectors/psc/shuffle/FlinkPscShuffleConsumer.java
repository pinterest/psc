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

package com.pinterest.flink.streaming.connectors.psc.shuffle;

import com.pinterest.flink.streaming.connectors.psc.config.OffsetCommitMode;
import com.pinterest.flink.streaming.connectors.psc.internals.AbstractFetcher;
import com.pinterest.flink.streaming.connectors.psc.internals.PscShuffleFetcher;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import com.pinterest.flink.streaming.connectors.psc.FlinkPscConsumer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;

import java.util.Map;
import java.util.Properties;

/**
 * Flink PSC Shuffle Consumer Function.
 */
@Internal
public class FlinkPscShuffleConsumer<T> extends FlinkPscConsumer<T> {
    private final TypeSerializer<T> typeSerializer;
    private final int producerParallelism;

    FlinkPscShuffleConsumer(
            String topicUri,
            TypeInformationSerializationSchema<T> schema,
            TypeSerializer<T> typeSerializer,
            Properties configuration) {
        // The schema is needed to call the right FlinkPscConsumer constructor.
        // It is never used, can be `null`, but `null` confuses the compiler.
        super(topicUri, schema, configuration);
        this.typeSerializer = typeSerializer;

        Preconditions.checkArgument(
                configuration.getProperty(FlinkPscShuffle.PRODUCER_PARALLELISM) != null,
                "Missing producer parallelism for PSC Shuffle");
        producerParallelism = PropertiesUtil.getInt(configuration, FlinkPscShuffle.PRODUCER_PARALLELISM, Integer.MAX_VALUE);
    }

    @Override
    protected AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<PscTopicUriPartition, Long> assignedPscTopicUriPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {
        // make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
        // this overwrites whatever setting the user configured in the properties
        adjustAutoCommitConfig(properties, offsetCommitMode);

        return new PscShuffleFetcher<>(
                sourceContext,
                assignedPscTopicUriPartitionsWithInitialOffsets,
                watermarkStrategy,
                runtimeContext.getProcessingTimeService(),
                runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                runtimeContext.getUserCodeClassLoader(),
                runtimeContext.getTaskNameWithSubtasks(),
                deserializer,
                properties,
                pollTimeout,
                runtimeContext.getMetricGroup(),
                consumerMetricGroup,
                useMetrics,
                typeSerializer,
                producerParallelism
            );
    }
}
