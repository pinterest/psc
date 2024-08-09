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

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.serde.ByteArraySerializer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder to construct {@link PscSink}.
 *
 * <p>The following example shows the minimum setup to create a PscSink that writes String values
 * to a topicUri.
 *
 * <pre>{@code
 * PscSInk<String> sink = PscSink
 *     .<String>builder
 *     .setRecordSerializer(MY_RECORD_SERIALIZER)
 *     .build();
 * }</pre>
 *
 * <p>One can also configure different {@link DeliveryGuarantee} by using {@link
 * #setDeliverGuarantee(DeliveryGuarantee)} but keep in mind when using {@link
 * DeliveryGuarantee#EXACTLY_ONCE} one must set the transactionalIdPrefix {@link
 * #setTransactionalIdPrefix(String)}.
 *
 * @see PscSink for a more detailed explanation of the different guarantees.
 * @param <IN> type of the records written
 */
@PublicEvolving
public class PscSinkBuilder<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(PscSinkBuilder.class);
    private static final Duration DEFAULT_PSC_TRANSACTION_TIMEOUT = Duration.ofHours(1);
    private static final int MAXIMUM_PREFIX_BYTES = 64000;

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
    private String transactionalIdPrefix = "psc-sink";

    private Properties pscProducerConfig;
    private PscRecordSerializationSchema<IN> recordSerializer;

    PscSinkBuilder() {}

    /**
     * Sets the wanted the {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * #deliveryGuarantee}.
     *
     * @param deliveryGuarantee
     * @return {@link PscSinkBuilder}
     */
    public PscSinkBuilder<IN> setDeliverGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        return this;
    }

    /**
     * Sets the configuration which used to instantiate all used {@link
     * com.pinterest.psc.producer.PscProducer}.
     *
     * @param pscProducerConfig
     * @return {@link PscSinkBuilder}
     */
    public PscSinkBuilder<IN> setPscProducerConfig(Properties pscProducerConfig) {
        this.pscProducerConfig = checkNotNull(pscProducerConfig, "pscProducerConfig");
        // set the producer configuration properties for PSC record key value serializers.
        if (!pscProducerConfig.containsKey(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER)) {
            pscProducerConfig.put(
                    PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER,
                    ByteArraySerializer.class.getName());
        } else {
            LOG.warn(
                    "Overwriting the '{}' is not recommended",
                    PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER);
        }

        if (!pscProducerConfig.containsKey(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER)) {
            pscProducerConfig.put(
                    PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER,
                    ByteArraySerializer.class.getName());
        } else {
            LOG.warn(
                    "Overwriting the '{}' is not recommended",
                    PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER);
        }

        if (!pscProducerConfig.containsKey(PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS)) {
            final long timeout = DEFAULT_PSC_TRANSACTION_TIMEOUT.toMillis();
            checkState(
                    timeout < Integer.MAX_VALUE && timeout > 0,
                    "timeout does not fit into 32 bit integer");
            pscProducerConfig.put(PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS, (int) timeout);
            LOG.warn(
                    "Property [{}] not specified. Setting it to {}",
                    PscConfiguration.PSC_PRODUCER_TRANSACTION_TIMEOUT_MS,
                    DEFAULT_PSC_TRANSACTION_TIMEOUT);
        }
        return this;
    }

    /**
     * Sets the {@link PscRecordSerializationSchema} that transforms incoming records to {@link
     * com.pinterest.psc.producer.PscProducerMessage}s.
     *
     * @param recordSerializer
     * @return {@link PscSinkBuilder}
     */
    public PscSinkBuilder<IN> setRecordSerializer(
            PscRecordSerializationSchema<IN> recordSerializer) {
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        ClosureCleaner.clean(
                this.recordSerializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    /**
     * Sets the prefix for all created transactionalIds if {@link DeliveryGuarantee#EXACTLY_ONCE} is
     * configured.
     *
     * <p>It is mandatory to always set this value with {@link DeliveryGuarantee#EXACTLY_ONCE} to
     * prevent corrupted transactions if multiple jobs using the PscSink run against the same
     * Kafka Cluster. The default prefix is {@link #transactionalIdPrefix}.
     *
     * <p>The size of the prefix is capped by {@link #MAXIMUM_PREFIX_BYTES} formatted with UTF-8.
     *
     * <p>It is important to keep the prefix stable across application restarts. If the prefix
     * changes it might happen that lingering transactions are not correctly aborted and newly
     * written messages are not immediately consumable until the transactions timeout.
     *
     * @param transactionalIdPrefix
     * @return {@link PscSinkBuilder}
     */
    public PscSinkBuilder<IN> setTransactionalIdPrefix(String transactionalIdPrefix) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        checkState(
                transactionalIdPrefix.getBytes(StandardCharsets.UTF_8).length
                        <= MAXIMUM_PREFIX_BYTES,
                "The configured prefix is too long and the resulting transactionalId might exceed Kafka's transactionalIds size.");
        return this;
    }

    private void sanityCheck() {
        if (pscProducerConfig == null) {
            setPscProducerConfig(new Properties());
        }
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            checkState(
                    transactionalIdPrefix != null,
                    "EXACTLY_ONCE delivery guarantee requires a transactionIdPrefix to be set to provide unique transaction names across multiple PscSinks writing to the same Kafka cluster.");
        }
        checkNotNull(recordSerializer, "recordSerializer");
    }

    /**
     * Constructs the {@link PscSink} with the configured properties.
     *
     * @return {@link PscSink}
     */
    public PscSink<IN> build() {
        sanityCheck();
        return new PscSink<>(
                deliveryGuarantee, pscProducerConfig, transactionalIdPrefix, recordSerializer);
    }
}
