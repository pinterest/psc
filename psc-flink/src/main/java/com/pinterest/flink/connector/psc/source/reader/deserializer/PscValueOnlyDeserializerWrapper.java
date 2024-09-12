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

import com.pinterest.psc.common.PscPlugin;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.serde.Deserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** A package private class to wrap {@link Deserializer}. */
class PscValueOnlyDeserializerWrapper<T> implements PscRecordDeserializationSchema<T> {

    private static final long serialVersionUID = 5409547407386004054L;

    private static final Logger LOG =
            LoggerFactory.getLogger(PscValueOnlyDeserializerWrapper.class);

    private final Class<? extends Deserializer<T>> deserializerClass;

    private final PscConfiguration config;

    private transient Deserializer<T> deserializer;

    PscValueOnlyDeserializerWrapper(
            Class<? extends Deserializer<T>> deserializerClass, PscConfiguration config) {
        this.deserializerClass = deserializerClass;
        this.config = config;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        ClassLoader userCodeClassLoader = context.getUserCodeClassLoader().asClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userCodeClassLoader)) {
            deserializer =
                    (Deserializer<T>)
                            InstantiationUtil.instantiate(
                                    deserializerClass.getName(),
                                    Deserializer.class,
                                    getClass().getClassLoader());

            if (deserializer != null) {
                ((PscPlugin) deserializer).configure(config);
            } else {
                // Always be false since this Deserializer is only used for value.
                deserializer.configure(config, false);
            }
        } catch (Exception e) {
            throw new IOException(
                    "Failed to instantiate the deserializer of class " + deserializerClass, e);
        }
    }

    @Override
    public void deserialize(PscConsumerMessage<byte[], byte[]> message, Collector<T> collector)
            throws IOException, DeserializerException {
        if (deserializer == null) {
            throw new IllegalStateException(
                    "The deserializer has not been created. Make sure the open() method has been "
                            + "invoked.");
        }

        T value = deserializer.deserialize(message.getValue());
        LOG.trace(
                "Deserialized [partition: {}-{}, offset: {}, timestamp: {}, value: {}]",
                message.getMessageId().getTopicUriPartition().getTopicUriAsString(),
                message.getMessageId().getTopicUriPartition().getPartition(),
                message.getMessageId().getOffset(),
                message.getMessageId().getTimestamp(),
                value);
        collector.collect(value);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.createTypeInfo(Deserializer.class, deserializerClass, 0, null, null);
    }
}
