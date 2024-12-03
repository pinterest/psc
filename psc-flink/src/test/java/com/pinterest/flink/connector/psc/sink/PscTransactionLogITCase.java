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

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.ByteArraySerializer;
import com.pinterest.psc.serde.IntegerSerializer;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static com.pinterest.flink.connector.psc.sink.PscTransactionLog.TransactionState.CompleteAbort;
import static com.pinterest.flink.connector.psc.sink.PscTransactionLog.TransactionState.CompleteCommit;
import static com.pinterest.flink.connector.psc.sink.PscTransactionLog.TransactionState.Empty;
import static com.pinterest.flink.connector.psc.sink.PscTransactionLog.TransactionState.Ongoing;
import static com.pinterest.flink.connector.psc.sink.PscTransactionLog.TransactionState.PrepareAbort;
import static com.pinterest.flink.connector.psc.sink.PscTransactionLog.TransactionState.PrepareCommit;
import static com.pinterest.flink.connector.psc.testutils.DockerImageVersions.KAFKA;
import static com.pinterest.flink.connector.psc.testutils.PscTestUtils.injectDiscoveryConfigs;
import static com.pinterest.flink.connector.psc.testutils.PscUtil.createKafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PscTransactionLog} to retrieve abortable PSC transactions. */
public class PscTransactionLogITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(PscSinkITCase.class);
    private static final String TOPIC_URI_STR = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER_URI.getTopicUriAsString() + "pscTransactionLogTest";
    private static final String TRANSACTIONAL_ID_PREFIX = "psc-log";

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG).withEmbeddedZookeeper();

    private final List<PscProducer<byte[], Integer>> openProducers = new ArrayList<>();

    @After
    public void tearDown() {
        openProducers.forEach(p -> {
            try {
                p.close();
            } catch (Exception e) {
                LOG.warn("Error closing producer", e);
            }
        });
    }

    @Test
    public void testGetTransactions() throws ConfigurationException, ConsumerException, ProducerException {
        committedTransaction(1);
        abortedTransaction(2);
        lingeringTransaction(3);
        lingeringTransaction(4);

        final PscTransactionLog transactionLog =
                new PscTransactionLog(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER_URI.getTopicUriAsString(), getPscClientConfiguration());
        final List<PscTransactionLog.TransactionRecord> transactions = transactionLog.getTransactions();
        assertThat(transactions)
                .containsExactlyInAnyOrder(
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(1), Empty),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(1), Ongoing),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(1), PrepareCommit),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(1), CompleteCommit),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(2), Empty),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(2), Ongoing),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(2), PrepareAbort),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(2), CompleteAbort),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(3), Empty),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(3), Ongoing),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(4), Empty),
                        new PscTransactionLog.TransactionRecord(buildTransactionalId(4), Ongoing));
    }

    private void committedTransaction(long id) throws ConfigurationException, ProducerException {
        submitTransaction(
                id,
                producer -> {
                    try {
                        producer.beginTransaction();
                        producer.send(new PscProducerMessage<>(TOPIC_URI_STR, 0, null, 1, null));
                        producer.flush();
                        producer.commitTransaction();
                        producer.flush();
                    } catch (ProducerException | ConfigurationException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void lingeringTransaction(long id) throws ConfigurationException, ProducerException {
        submitTransaction(
                id,
                producer -> {
                    try {
                        producer.beginTransaction();
                        producer.send(new PscProducerMessage<>(TOPIC_URI_STR, 0, null, 1, null));
                        producer.flush();
                    } catch (ConfigurationException | ProducerException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void abortedTransaction(long id) throws ConfigurationException, ProducerException {
        submitTransaction(
                id,
                producer -> {
                    try {
                        producer.beginTransaction();
                        producer.send(new PscProducerMessage<>(TOPIC_URI_STR, 0, null, 1, null));
                        producer.flush();
                        producer.abortTransaction();
                        producer.flush();
                    } catch (ProducerException | ConfigurationException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void submitTransaction(long id, Consumer<PscProducer<byte[], Integer>> producerAction) throws ConfigurationException, ProducerException {
        PscProducer<byte[], Integer> producer = createProducer(buildTransactionalId(id));
        openProducers.add(producer);
        producerAction.accept(producer);
        // don't close here for lingering transactions
    }

    private static String buildTransactionalId(long id) {
        return TRANSACTIONAL_ID_PREFIX + id;
    }

    private static PscProducer<byte[], Integer> createProducer(String transactionalId) throws ConfigurationException, ProducerException {
        final Properties producerProperties = getPscClientConfiguration();
        producerProperties.put(
                PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getName());
        producerProperties.put(
                PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, IntegerSerializer.class.getName());
        producerProperties.put(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, transactionalId);
        producerProperties.put(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER_URI.getTopicUriAsString());
        return new PscProducer<>(PscConfigurationUtils.propertiesToPscConfiguration(producerProperties));
    }

    private static Properties getPscClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put(PscConfiguration.PSC_CONSUMER_GROUP_ID, "flink-tests");
        standardProps.put(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, false);
        standardProps.put(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);
        standardProps.put(PscConfiguration.PSC_CONSUMER_PARTITION_FETCH_MAX_BYTES, 256);
        standardProps.put(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "flink-tests");
        injectDiscoveryConfigs(standardProps, KAFKA_CONTAINER.getBootstrapServers(), PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER_URI.getTopicUriAsString());
        return standardProps;
    }
}
