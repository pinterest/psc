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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * A test for the {@link TypeSerializer TypeSerializers} used for
 * {@link FlinkPscProducer.NextTransactionalIdHint}.
 */
class NextTransactionalIdHintSerializerTest extends
        SerializerTestBase<FlinkPscProducer.NextTransactionalIdHint> {

    @Override
    protected TypeSerializer<FlinkPscProducer.NextTransactionalIdHint> createSerializer() {
        return new FlinkPscProducer.NextTransactionalIdHintSerializer();
    }

    @Override
    protected int getLength() {
        return Long.BYTES + Integer.BYTES;
    }

    @Override
    protected Class<FlinkPscProducer.NextTransactionalIdHint> getTypeClass() {
        return (Class) FlinkPscProducer.NextTransactionalIdHint.class;
    }

    @Override
    protected FlinkPscProducer.NextTransactionalIdHint[] getTestData() {
        return new FlinkPscProducer.NextTransactionalIdHint[]{
                new FlinkPscProducer.NextTransactionalIdHint(1, 0L),
                new FlinkPscProducer.NextTransactionalIdHint(1, 1L),
                new FlinkPscProducer.NextTransactionalIdHint(1, -1L),
                new FlinkPscProducer.NextTransactionalIdHint(2, 0L),
                new FlinkPscProducer.NextTransactionalIdHint(2, 1L),
                new FlinkPscProducer.NextTransactionalIdHint(2, -1L),
        };
    }
}
