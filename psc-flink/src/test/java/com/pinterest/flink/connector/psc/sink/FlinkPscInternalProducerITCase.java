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
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationUtils;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.pinterest.flink.connector.psc.testutils.PscUtil.createKafkaContainer;
import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class FlinkPscInternalProducerITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(FlinkPscInternalProducerITCase.class);

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG).withEmbeddedZookeeper();

    private static final String TRANSACTION_PREFIX = "test-transaction-";

    private static final String CLUSTER_URI = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region1::cluster1:";

    @Test
    void testInitTransactionId() throws IOException {
        final String topicUriStr = CLUSTER_URI + "test-init-transactions";
        FlinkPscInternalProducer<String, String> reuse = null;
        try {
            int numTransactions = 20;
            reuse = new FlinkPscInternalProducer<>(getProperties(), "dummy");
            for (int i = 1; i <= numTransactions; i++) {
                reuse.initTransactionId(TRANSACTION_PREFIX + i);
                reuse.beginTransaction();
                reuse.send(new PscProducerMessage<>(topicUriStr, "test-value-" + i));
                if (i % 2 == 0) {
                    reuse.commitTransaction();
                } else {
                    reuse.flush();
                    reuse.abortTransaction();
                }
                assertNumTransactions(i);
                assertThat(readRecords(topicUriStr).asList().size()).isEqualTo(i / 2);
            }
        } catch (ConfigurationException | ProducerException | TopicUriSyntaxException | ConsumerException e) {
            throw new RuntimeException(e);
        } finally {
            if (reuse != null)
                reuse.close();
        }
    }

    @ParameterizedTest
    @MethodSource("provideTransactionsFinalizer")
    void testResetInnerTransactionIfFinalizingTransactionFailed(
            Consumer<FlinkPscInternalProducer<?, ?>> transactionFinalizer) {
        final String topicUriStr = CLUSTER_URI + "reset-producer-internal-state";   // TODO: create discovery mechanism for topic
        try (FlinkPscInternalProducer<String, String> fenced =
                new FlinkPscInternalProducer<>(getProperties(), "dummy")) {
            fenced.initTransactions();
            fenced.beginTransaction();
            fenced.send(new PscProducerMessage<>(topicUriStr, "test-value"));
            // Start a second producer that fences the first one
            try (FlinkPscInternalProducer<String, String> producer =
                    new FlinkPscInternalProducer<>(getProperties(), "dummy")) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new PscProducerMessage<>(topicUriStr, "test-value"));
                producer.commitTransaction();
            }
            assertThatThrownBy(() -> transactionFinalizer.accept(fenced)).getRootCause().isInstanceOf(ProducerFencedException.class);
            // Internal transaction should be reset and setting a new transactional id is possible
            fenced.setTransactionId("dummy2");
        } catch (ConfigurationException | ProducerException | TopicUriSyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(PscConfiguration.PSC_PRODUCER_IDEMPOTENCE_ENABLED, "true");
        properties.put(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "FlinkPscInternalProducerITCase");
        properties.put(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "FlinkPscInternalProducerITCase");
        properties.put(PscConfiguration.PSC_CONSUMER_GROUP_ID, "FlinkPscInternalProducerITCase");
        properties.put(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        properties.put(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());
        properties.put(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        properties.put(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        properties.put(PscFlinkConfiguration.CLUSTER_URI_CONFIG, CLUSTER_URI);
        String brokerConnectionString = KAFKA_CONTAINER.getBootstrapServers().split("://")[1];
        int bootstrapCount = brokerConnectionString.split(",").length;
        properties.setProperty("psc.discovery.topic.uri.prefixes",
                StringUtils.repeat(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX, ",", bootstrapCount));
        properties.setProperty("psc.discovery.connection.urls", brokerConnectionString);
        properties.setProperty("psc.discovery.security.protocols",
                StringUtils.repeat("plaintext", ",", bootstrapCount));
        return properties;
    }

    private static List<ThrowingConsumer<FlinkPscInternalProducer<?, ?>>> provideTransactionsFinalizer() {
        return Lists.newArrayList(
                FlinkPscInternalProducer::commitTransaction,
                FlinkPscInternalProducer::abortTransaction);
    }

    private void assertNumTransactions(int numTransactions) throws ConfigurationException, ConsumerException {
        List<PscTransactionLog.TransactionRecord> transactions =
                new PscTransactionLog(CLUSTER_URI, getProperties())
                        .getTransactions(id -> id.startsWith(TRANSACTION_PREFIX));
        assertThat(
                        transactions.stream()
                                .map(PscTransactionLog.TransactionRecord::getTransactionId)
                                .collect(Collectors.toSet()))
                .hasSize(numTransactions);
    }

    private PscConsumerPollMessageIterator<String, String> readRecords(String topicUri) throws ConfigurationException, ConsumerException {
        Properties properties = getProperties();
        properties.put(PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL, PscConfiguration.PSC_CONSUMER_ISOLATION_LEVEL_TRANSACTIONAL);
        PscConsumer<String, String> consumer = new PscConsumer<>(PscConfigurationUtils.propertiesToPscConfiguration(properties));
        consumer.assign(
                consumer.getPartitions(topicUri));
        consumer.seekToBeginning(consumer.assignment());
        return consumer.poll(Duration.ofMillis(1000));
    }

    public interface ThrowingConsumer<T> extends Consumer<T> {

        @Override
        default void accept(T t) {
            try {
                acceptThrows(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        void acceptThrows(T t) throws Exception;
    }
}
