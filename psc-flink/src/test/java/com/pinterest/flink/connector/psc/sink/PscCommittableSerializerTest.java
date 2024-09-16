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

package com.pinterest.flink.connector.psc.sink;

import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for serializing and deserialzing {@link PscCommittable} with {@link
 * PscCommittableSerializer}.
 */
public class PscCommittableSerializerTest extends TestLogger {

    private static final PscCommittableSerializer SERIALIZER = new PscCommittableSerializer();

    @Test
    public void testCommittableSerDe() throws IOException {
        final String transactionalId = "test-id";
        final short epoch = 5;
        final PscCommittable committable = new PscCommittable(1L, epoch, transactionalId, null);
        final byte[] serialized = SERIALIZER.serialize(committable);
        assertEquals(committable, SERIALIZER.deserialize(1, serialized));
    }
}
