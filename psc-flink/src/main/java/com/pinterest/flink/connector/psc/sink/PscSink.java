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

import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * Flink Sink to produce data into a PSC topicUri. The sink supports all delivery guarantees
 * described by {@link DeliveryGuarantee}.
 * <li>{@link DeliveryGuarantee#NONE} does not provide any guarantees: messages may be lost in case
 *     of issues on the Kafka broker and messages may be duplicated in case of a Flink failure.
 * <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} the sink will wait for all outstanding records in the
 *     PSC buffers to be acknowledged by the PSC producer on a checkpoint. No messages will be
 *     lost in case of any issue with the brokers but messages may be duplicated when Flink
 *     restarts.
 * <li>{@link DeliveryGuarantee#EXACTLY_ONCE}: Note that this mode is only supported by KafkaProducers in the backend.
 *
 *     In this mode the PscSink will write all messages in
 *     a Kafka transaction that will be committed to Kafka on a checkpoint. Thus, if the consumer
 *     reads only committed data (see Kafka consumer config isolation.level), no duplicates will be
 *     seen in case of a Flink restart. However, this delays record writing effectively until a
 *     checkpoint is written, so adjust the checkpoint duration accordingly. Please ensure that you
 *     use unique {@link #transactionalIdPrefix}s across your applications running on the same Kafka
 *     cluster such that multiple running jobs do not interfere in their transactions! Additionally,
 *     it is highly recommended to tweak Kafka transaction timeout (link) >> maximum checkpoint
 *     duration + maximum restart duration or data loss may happen when Kafka expires an uncommitted
 *     transaction.
 *
 * @param <IN> type of the records written
 * @see PscSinkBuilder on how to construct a PscSink
 */
@PublicEvolving
public class PscSink<IN>
        implements StatefulSink<IN, PscWriterState>,
                TwoPhaseCommittingSink<IN, PscCommittable> {

    private final DeliveryGuarantee deliveryGuarantee;

    private final PscRecordSerializationSchema<IN> recordSerializer;
    private final Properties pscProducerConfig;
    private final String transactionalIdPrefix;

    PscSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties pscProducerConfig,
            String transactionalIdPrefix,
            PscRecordSerializationSchema<IN> recordSerializer) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.pscProducerConfig = pscProducerConfig;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.recordSerializer = recordSerializer;
    }

    /**
     * Create a {@link PscSinkBuilder} to construct a new {@link PscSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link PscSinkBuilder}
     */
    public static <IN> PscSinkBuilder<IN> builder() {
        return new PscSinkBuilder<>();
    }

    @Internal
    @Override
    public Committer<PscCommittable> createCommitter() throws IOException {
        return new PscCommitter(pscProducerConfig);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<PscCommittable> getCommittableSerializer() {
        return new PscCommittableSerializer();
    }

    @Internal
    @Override
    public PscWriter<IN> createWriter(InitContext context) throws IOException {
        try {
            return new PscWriter<IN>(
                    deliveryGuarantee,
                    pscProducerConfig,
                    transactionalIdPrefix,
                    context,
                    recordSerializer,
                    context.asSerializationSchemaInitializationContext(),
                    Collections.emptyList());
        } catch (ConfigurationException | ClientException e) {
            throw new RuntimeException("Failed to createWriter", e);
        }
    }

    @Internal
    @Override
    public PscWriter<IN> restoreWriter(
            InitContext context, Collection<PscWriterState> recoveredState) throws IOException {
        try {
            return new PscWriter<>(
                    deliveryGuarantee,
                    pscProducerConfig,
                    transactionalIdPrefix,
                    context,
                    recordSerializer,
                    context.asSerializationSchemaInitializationContext(),
                    recoveredState);
        } catch (ConfigurationException | ClientException e) {
            throw new RuntimeException("Failed to restoreWriter", e);
        }
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<PscWriterState> getWriterStateSerializer() {
        return new PscWriterStateSerializer();
    }
}
