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
import com.pinterest.psc.consumer.PscConsumerMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * A wrapper class that wraps a {@link
 * PscDeserializationSchema} to deserialize {@link
 * PscConsumerMessage messages}.
 *
 * @param <T> the type of the deserialized records.
 */
class PscDeserializationSchemaWrapper<T> implements PscRecordDeserializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private final PscDeserializationSchema<T> pscDeserializationSchema;

    PscDeserializationSchemaWrapper(PscDeserializationSchema<T> pscDeserializationSchema) {
        this.pscDeserializationSchema = pscDeserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        pscDeserializationSchema.open(context);
    }

    @Override
    public void deserialize(PscConsumerMessage<byte[], byte[]> message, Collector<T> out)
            throws IOException {
        try {
            pscDeserializationSchema.deserialize(message, out);
        } catch (Exception exception) {
            throw new IOException(
                    String.format("Failed to deserialize consumer record %s.", message), exception);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return pscDeserializationSchema.getProducedType();
    }
}
