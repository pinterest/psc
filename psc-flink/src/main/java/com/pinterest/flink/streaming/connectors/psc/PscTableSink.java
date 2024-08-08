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

package com.pinterest.flink.streaming.connectors.psc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * Kafka table sink for writing data into Kafka.
 */

// TODO: migration - remove

@Internal
public class PscTableSink extends PscTableSinkBase {

    public PscTableSink(
            TableSchema schema,
            String topicUri,
            Properties properties,
            Optional<FlinkPscPartitioner<Row>> partitioner,
            SerializationSchema<Row> serializationSchema) {

        super(schema, topicUri, properties, partitioner, serializationSchema);
    }

    @Override
    protected SinkFunction<Row> createPscProducer(
            String topicUri,
            Properties properties,
            SerializationSchema<Row> serializationSchema,
            Optional<FlinkPscPartitioner<Row>> partitioner) {
        return new FlinkPscProducer<>(
                topicUri,
                serializationSchema,
                properties,
                partitioner);
    }
}
