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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

public class PscSimpleTypeSerializerSnapshot<T> extends SimpleTypeSerializerSnapshot<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PscSimpleTypeSerializerSnapshot.class);

    public PscSimpleTypeSerializerSnapshot(@Nonnull Supplier<? extends TypeSerializer<T>> serializerSupplier) {
        super(serializerSupplier);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
        // default case
        if (super.resolveSchemaCompatibility(newSerializer).isCompatibleAsIs())
            return TypeSerializerSchemaCompatibility.compatibleAsIs();

        // customized compatibility
        String newSerializerClassName = newSerializer.getClass().getName();
        String restoreSerializerClassName = restoreSerializer().getClass().getName();
        if (newSerializerClassName.contains("Kafka") && restoreSerializerClassName.contains("Psc") && newSerializerClassName.contains("$") &&
                newSerializerClassName.replaceAll("Kafka", "Psc").replaceAll("kafka", "psc").equals(restoreSerializerClassName)) {
            try {
                Class<?> clazz = Class.forName(newSerializerClassName);
                Object serializer = clazz.newInstance();
                LOG.info("Matched Kafka's type serializer schema with PSC's: {} -> {}", restoreSerializerClassName, newSerializerClassName);
                return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer((TypeSerializer <T>) serializer);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return TypeSerializerSchemaCompatibility.incompatible();
    }
}
