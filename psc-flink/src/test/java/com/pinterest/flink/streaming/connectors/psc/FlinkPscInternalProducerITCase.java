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

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.streaming.connectors.psc.internals.FlinkPscInternalProducer;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for our own {@link FlinkPscInternalProducer}.
 */
@SuppressWarnings("serial")
public class FlinkPscInternalProducerITCase extends PscTestBaseWithKafkaAsPubSub {
    protected String transactionalId;
    protected Properties extraProducerProperties;
    protected Properties extraConsumerProperties;

    @BeforeClass
    public static void prepare() throws Exception {
        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Starting KafkaTestBase ");
        LOG.info("-------------------------------------------------------------------------");

        Properties serverProperties = new Properties();
        serverProperties.put("transaction.state.log.num.partitions", Integer.toString(1));
        serverProperties.put("auto.leader.rebalance.enable", Boolean.toString(false));
        startClusters(PscTestEnvironmentWithKafkaAsPubSub.createConfig()
                .setKafkaServersNumber(NUMBER_OF_KAFKA_SERVERS)
                .setSecureMode(false)
                .setHideKafkaBehindProxy(true)
                .setKafkaServerProperties(serverProperties));
    }

    @Before
    public void before() {
        transactionalId = UUID.randomUUID().toString();
        extraProducerProperties = new Properties();
        extraProducerProperties.putAll(standardPscProducerConfiguration);
        extraProducerProperties.putAll(pscDiscoveryConfiguration);
        extraProducerProperties.put(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, transactionalId);
        extraProducerProperties.put(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        extraProducerProperties.put(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());

        extraConsumerProperties = new Properties();
        extraConsumerProperties.putAll(standardPscConsumerConfiguration);
        extraConsumerProperties.putAll(pscDiscoveryConfiguration);
        extraConsumerProperties.put(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        extraConsumerProperties.put(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        extraConsumerProperties.put("psc.consumer.isolation.level", "read_committed");
    }

    @Test(timeout = 30000L)
    public void testHappyPath() throws IOException, ProducerException, ConfigurationException, ConsumerException {
        String topic = "flink-kafka-producer-happy-path";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        PscProducer<String, String> pscProducer = new FlinkPscInternalProducer<>(extraProducerProperties);
        try {
            //pscProducer.initTransactions();
            pscProducer.beginTransaction();
            pscProducer.send(new PscProducerMessage<>(topicUri, "42", "42"));
            pscProducer.commitTransaction();
        } finally {
            pscProducer.close(Duration.ofSeconds(5));
        }
        assertRecord(topicUri, "42", "42");
        deleteTestTopic(topic);
    }

    @Test(timeout = 30000L)
    public void testResumeTransaction() throws ProducerException, ConfigurationException, ConsumerException, IOException {
        String topic = "flink-kafka-producer-resume-transaction";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        FlinkPscInternalProducer<String, String> pscProducer = new FlinkPscInternalProducer<>(extraProducerProperties);
        try {
            PscProducerMessage<String, String> pscProducerMessage = new PscProducerMessage<>(topicUri, "42", "42");
            pscProducer.beginTransaction();
            pscProducer.send(pscProducerMessage);
            pscProducer.flush();

            FlinkPscInternalProducer<String, String> resumeProducer = new FlinkPscInternalProducer<>(extraProducerProperties);
            try {
                resumeProducer.resumeTransaction(pscProducer);
                resumeProducer.commitTransaction();
            } finally {
                resumeProducer.close(Duration.ofSeconds(5));
            }

            assertRecord(topicUri, "42", "42");

            // this shouldn't throw - in case of network split, old producer might attempt to commit it's transaction
            pscProducer.commitTransaction();

            // this shouldn't fail also, for same reason as above
            resumeProducer = new FlinkPscInternalProducer<>(extraProducerProperties);
            try {
                resumeProducer.resumeTransaction(pscProducer);
                resumeProducer.commitTransaction();
            } finally {
                resumeProducer.close(Duration.ofSeconds(5));
            }
        } finally {
            pscProducer.close(Duration.ofSeconds(5));
        }
        deleteTestTopic(topic);
    }

    @Test(timeout = 30000L, expected = IllegalStateException.class)
    public void testPartitionsForAfterClosed() throws ProducerException, ConfigurationException, IOException {
        FlinkPscInternalProducer<String, String> pscProducer = new FlinkPscInternalProducer<>(extraProducerProperties);
        pscProducer.close(Duration.ofSeconds(5));
        pscProducer.getPartitions("Topic");
    }

    @Test(timeout = 30000L, expected = IllegalStateException.class)
    public void testBeginTransactionAfterClosed() throws ProducerException, ConfigurationException, IOException {
        FlinkPscInternalProducer<String, String> pscProducer = new FlinkPscInternalProducer<>(extraProducerProperties);
        //pscProducer.initTransactions();
        pscProducer.close(Duration.ofSeconds(5));
        pscProducer.beginTransaction();
    }

    @Test(timeout = 30000L, expected = IllegalStateException.class)
    public void testCommitTransactionAfterClosed() throws ProducerException, ConfigurationException, IOException {
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + "testCommitTransactionAfterClosed";
        FlinkPscInternalProducer<String, String> pscProducer = getClosedProducer(topicUri);
        pscProducer.commitTransaction();
    }

    @Test(timeout = 30000L, expected = ProducerException.class)
    public void testResumeTransactionAfterClosed() throws ProducerException, ConfigurationException, IOException {
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + "testAbortTransactionAfterClosed";
        FlinkPscInternalProducer<String, String> pscProducer = getClosedProducer(topicUri);
        pscProducer.resumeTransaction(pscProducer);
    }

    @Test(timeout = 30000L, expected = IllegalStateException.class)
    public void testAbortTransactionAfterClosed() throws ProducerException, ConfigurationException, IOException {
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + "testAbortTransactionAfterClosed";
        FlinkPscInternalProducer<String, String> pscProducer = getClosedProducer(topicUri);
        pscProducer.abortTransaction();
        pscProducer.resumeTransaction(pscProducer);
    }

    @Test(timeout = 30000L, expected = ProducerException.class)
    public void testFlushAfterClosed() throws ProducerException, ConfigurationException, IOException {
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + "testCommitTransactionAfterClosed";
        FlinkPscInternalProducer<String, String> pscProducer = getClosedProducer(topicUri);
        pscProducer.flush();
    }

    @Test(timeout = 30000L)
    public void testProducerWhenCommitEmptyPartitionsToOutdatedTxnCoordinator() throws Exception {
        String topic = "flink-kafka-producer-txn-coordinator-changed";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topicUri, 1, 2);
        PscProducer<String, String> pscProducer = new FlinkPscInternalProducer<>(extraProducerProperties);
        try {
            // <added> a simple transactional call to launch a backend producer
            pscProducer.beginTransaction();
            PscProducerMessage<String, String> pscProducerMessage = new PscProducerMessage<>(
                    topicUri,
                    "message value"
            );
            pscProducer.send(pscProducerMessage);
            pscProducer.abortTransaction();
            // </added>

            pscProducer.beginTransaction();
            restartBroker(pscTestEnvWithKafka.getLeaderToShutDown("__transaction_state"));
            pscProducer.flush();
            pscProducer.commitTransaction();
        } finally {
            pscProducer.close(Duration.ofSeconds(5));
        }
        deleteTestTopic(topic);
    }

    private FlinkPscInternalProducer<String, String> getClosedProducer(String topicUri) throws ProducerException, ConfigurationException, IOException {
        FlinkPscInternalProducer<String, String> pscProducer = new FlinkPscInternalProducer<>(extraProducerProperties);
        pscProducer.beginTransaction();
        pscProducer.send(new PscProducerMessage<>(topicUri, "42", "42"));
        pscProducer.close(Duration.ofSeconds(5));
        return pscProducer;
    }

    private void assertRecord(String topicUri, String expectedKey, String expectedValue) throws ConsumerException, ConfigurationException {
        PscConfiguration pscConfiguration = new PscConfiguration();
        extraConsumerProperties.forEach((key, value) -> pscConfiguration.setProperty(key.toString(), value));
        try (PscConsumer<String, String> pscConsumer = new PscConsumer<>(pscConfiguration)) {
            pscConsumer.subscribe(Collections.singletonList(topicUri));
            PscConsumerPollMessageIterator<String, String> messages = pscConsumer.poll(Duration.ofMillis(10000));

            assertTrue(messages.hasNext());
            PscConsumerMessage<String, String> message = messages.next();
            assertEquals(expectedKey, message.getKey());
            assertEquals(expectedValue, message.getValue());
        }
    }

    private void restartBroker(int brokerId) throws Exception {
        pscTestEnvWithKafka.stopBroker(brokerId);
        pscTestEnvWithKafka.restartBroker(brokerId);
    }
}
