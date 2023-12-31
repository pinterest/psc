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
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction.TransactionHolder;

import java.util.Collections;
import java.util.Optional;

/**
 * A test for the {@link TypeSerializer TypeSerializers} used for the Kafka producer state.
 */
public class FlinkPscProducerStateSerializerTest
        extends SerializerTestBase<
        TwoPhaseCommitSinkFunction.State<
                FlinkPscProducer.PscTransactionState,
                FlinkPscProducer.PscTransactionContext>> {

    @Override
    protected TypeSerializer<
            TwoPhaseCommitSinkFunction.State<
                    FlinkPscProducer.PscTransactionState,
                    FlinkPscProducer.PscTransactionContext>> createSerializer() {
        return new TwoPhaseCommitSinkFunction.StateSerializer<>(
                new FlinkPscProducer.TransactionStateSerializer(),
                new FlinkPscProducer.ContextStateSerializer());
    }

    @Override
    protected Class<TwoPhaseCommitSinkFunction.State<
            FlinkPscProducer.PscTransactionState,
            FlinkPscProducer.PscTransactionContext>> getTypeClass() {
        return (Class) TwoPhaseCommitSinkFunction.State.class;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected TwoPhaseCommitSinkFunction.State<
            FlinkPscProducer.PscTransactionState,
            FlinkPscProducer.PscTransactionContext>[] getTestData() {
        //noinspection unchecked
        return new TwoPhaseCommitSinkFunction.State[]{
                new TwoPhaseCommitSinkFunction.State<
                        FlinkPscProducer.PscTransactionState,
                        FlinkPscProducer.PscTransactionContext>(
                        new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 0),
                        Collections.emptyList(),
                        Optional.empty()),
                new TwoPhaseCommitSinkFunction.State<
                        FlinkPscProducer.PscTransactionState,
                        FlinkPscProducer.PscTransactionContext>(
                        new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 2711),
                        Collections.singletonList(new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 42)),
                        Optional.empty()),
                new TwoPhaseCommitSinkFunction.State<
                        FlinkPscProducer.PscTransactionState,
                        FlinkPscProducer.PscTransactionContext>(
                        new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 0),
                        Collections.emptyList(),
                        Optional.of(new FlinkPscProducer.PscTransactionContext(Collections.emptySet()))),
                new TwoPhaseCommitSinkFunction.State<
                        FlinkPscProducer.PscTransactionState,
                        FlinkPscProducer.PscTransactionContext>(
                        new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 0),
                        Collections.emptyList(),
                        Optional.of(new FlinkPscProducer.PscTransactionContext(Collections.singleton("hello")))),
                new TwoPhaseCommitSinkFunction.State<
                        FlinkPscProducer.PscTransactionState,
                        FlinkPscProducer.PscTransactionContext>(
                        new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 0),
                        Collections.singletonList(new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 0)),
                        Optional.of(new FlinkPscProducer.PscTransactionContext(Collections.emptySet()))),
                new TwoPhaseCommitSinkFunction.State<
                        FlinkPscProducer.PscTransactionState,
                        FlinkPscProducer.PscTransactionContext>(
                        new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 0),
                        Collections.singletonList(new TransactionHolder(new FlinkPscProducer.PscTransactionState("fake", 1L, (short) 42, null), 0)),
                        Optional.of(new FlinkPscProducer.PscTransactionContext(Collections.singleton("hello"))))
        };
    }

    @Override
    public void testInstantiate() {
        // this serializer does not support instantiation
    }
}
