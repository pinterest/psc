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

package com.pinterest.flink.connector.psc.source.reader.deserializer;

import com.pinterest.flink.streaming.connectors.psc.PscDeserializationSchema;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.serde.Deserializer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/** An interface for the deserialization of Kafka records. */
@PublicEvolving
public interface PscRecordDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes the byte message.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param message The ConsumerRecord to deserialize.
     * @param out The collector to put the resulting messages.
     */
    void deserialize(PscConsumerMessage<byte[], byte[]> message, Collector<T> out) throws IOException, DeserializerException;

    /**
     * Wraps a legacy {@link PscDeserializationSchema} as the deserializer of the {@link
     * PscConsumerMessage messages}.
     *
     * <p>Note that the {@link PscDeserializationSchema#isEndOfStream(Object)} method will no
     * longer be used to determine the end of the stream.
     *
     * @param pscDeserializationSchema the legacy {@link PscDeserializationSchema} to use.
     * @param <V> the return type of the deserialized record.
     * @return A {@link PscRecordDeserializationSchema} that uses the given {@link
     *     PscDeserializationSchema} to deserialize the {@link PscConsumerMessage ConsumerRecords}.
     */
    static <V> PscRecordDeserializationSchema<V> of(
            PscDeserializationSchema<V> pscDeserializationSchema) {
        return new PscDeserializationSchemaWrapper<>(pscDeserializationSchema);
    }

    /**
     * Wraps a {@link DeserializationSchema} as the value deserialization schema of the {@link
     * PscConsumerMessage messages}. The other fields such as key, headers, timestamp are
     * ignored.
     *
     * <p>Note that the {@link DeserializationSchema#isEndOfStream(Object)} method will no longer be
     * used to determine the end of the stream.
     *
     * @param valueDeserializationSchema the {@link DeserializationSchema} used to deserialized the
     *     value of a {@link PscConsumerMessage}.
     * @param <V> the type of the deserialized record.
     * @return A {@link PscRecordDeserializationSchema} that uses the given {@link
     *     DeserializationSchema} to deserialize a {@link PscConsumerMessage} from its value.
     */
    static <V> PscRecordDeserializationSchema<V> valueOnly(
            DeserializationSchema<V> valueDeserializationSchema) {
        return new PscValueOnlyDeserializationSchemaWrapper<>(valueDeserializationSchema);
    }

    /**
     * Wraps a Kafka {@link Deserializer} to a {@link PscRecordDeserializationSchema}.
     *
     * @param valueDeserializerClass the deserializer class used to deserialize the value.
     * @param <V> the value type.
     * @return A {@link PscRecordDeserializationSchema} that deserialize the value with the given
     *     deserializer.
     */
    static <V> PscRecordDeserializationSchema<V> valueOnly(
            Class<? extends Deserializer<V>> valueDeserializerClass) {
        return valueOnly(valueDeserializerClass, new PscConfiguration());
    }

    /**
     * Wraps a Kafka {@link Deserializer} to a {@link PscRecordDeserializationSchema}.
     *
     * @param valueDeserializerClass the deserializer class used to deserialize the value.
     * @param config the configuration of the value deserializer. If the deserializer is an
     *     implementation of {@code Configurable}, the configuring logic will be handled by {@link
     *     org.apache.kafka.common.Configurable#configure(Map)} with the given config,
     *     otherwise {@link Deserializer#configure(com.pinterest.psc.config.PscConfiguration, boolean)} will be invoked.
     * @param <V> the value type.
     * @param <D> the type of the deserializer.
     * @return A {@link PscRecordDeserializationSchema} that deserialize the value with the given
     *     deserializer.
     */
    static <V, D extends Deserializer<V>> PscRecordDeserializationSchema<V> valueOnly(
            Class<D> valueDeserializerClass, PscConfiguration config) {
        return new PscValueOnlyDeserializerWrapper<>(valueDeserializerClass, config);
    }
}
