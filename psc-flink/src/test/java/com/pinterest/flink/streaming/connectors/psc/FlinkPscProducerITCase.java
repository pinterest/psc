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

import com.pinterest.flink.streaming.connectors.psc.internals.KeyedSerializationSchemaWrapper;
import com.pinterest.flink.streaming.util.serialization.psc.KeyedSerializationSchema;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.ExceptionUtils.findSerializedThrowable;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * IT cases for the {@link FlinkPscProducer}.
 */
public class FlinkPscProducerITCase extends PscTestBaseWithKafkaAsPubSub {

    protected String transactionalId;
    protected Properties extraProducerProperties;
    protected Properties extraConsumerProperties;

    protected TypeInformationSerializationSchema<Integer> integerSerializationSchema =
            new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
    protected KeyedSerializationSchema<Integer> integerKeyedSerializationSchema =
            new KeyedSerializationSchemaWrapper<>(integerSerializationSchema);

    @Before
    public void before() {
        transactionalId = UUID.randomUUID().toString();
        extraProducerProperties = new Properties();
        extraProducerProperties.putAll(standardPscProducerConfiguration);
        extraProducerProperties.put(PscConfiguration.PSC_PRODUCER_TRANSACTIONAL_ID, transactionalId);
        extraProducerProperties.put(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, StringSerializer.class.getName());
        extraProducerProperties.put(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());

        extraConsumerProperties = new Properties();
        extraConsumerProperties.putAll(standardPscConsumerConfiguration);
        extraConsumerProperties.put(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, StringDeserializer.class.getName());
        extraConsumerProperties.put(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        extraConsumerProperties.put("psc.consumer.isolation.level", "read_committed");
    }

    @Test
    public void resourceCleanUpNone() throws Exception {
        resourceCleanUp(FlinkPscProducer.Semantic.NONE);
    }

    @Test
    public void resourceCleanUpAtLeastOnce() throws Exception {
        resourceCleanUp(FlinkPscProducer.Semantic.AT_LEAST_ONCE);
    }

    /**
     * This tests checks whether there is some resource leak in form of growing threads number.
     */
    public void resourceCleanUp(FlinkPscProducer.Semantic semantic) throws Exception {
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX +
                "flink-kafka-producer-resource-cleanup-" + semantic;

        final int allowedEpsilonThreadCountGrow = 50;

        Optional<Integer> initialActiveThreads = Optional.empty();
        for (int i = 0; i < allowedEpsilonThreadCountGrow * 2; i++) {
            try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness1 =
                         createTestHarness(topicUri, 1, 1, 0, semantic)) {
                testHarness1.setup();
                testHarness1.open();
            }

            if (initialActiveThreads.isPresent()) {
                assertThat("active threads count",
                        Thread.activeCount(),
                        lessThan(initialActiveThreads.get() + allowedEpsilonThreadCountGrow));
            } else {
                initialActiveThreads = Optional.of(Thread.activeCount());
            }
        }
        checkProducerLeak();
    }

    /**
     * This test ensures that transactions reusing transactional.ids (after returning to the pool) will not clash
     * with previous transactions using same transactional.ids.
     */
    @Test
    public void testRestoreToCheckpointAfterExceedingProducersPool() throws Exception {
        String topic = "flink-kafka-producer-fail-before-notify";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness1 = createTestHarness(topicUri)) {
            testHarness1.setup();
            testHarness1.open();
            testHarness1.processElement(42, 0);
            OperatorSubtaskState snapshot = testHarness1.snapshot(0, 0);
            testHarness1.processElement(43, 0);
            testHarness1.notifyOfCompletedCheckpoint(0);
            try {
                for (int i = 0; i < FlinkPscProducer.DEFAULT_PSC_PRODUCERS_POOL_SIZE; i++) {
                    testHarness1.snapshot(i + 1, 0);
                    testHarness1.processElement(i, 0);
                }
                throw new IllegalStateException("This should not be reached.");
            } catch (Exception ex) {
                if (!isCausedBy(FlinkPscErrorCode.PRODUCERS_POOL_EMPTY, ex)) {
                    throw ex;
                }
            }

            // Resume transactions before testHarness1 is being closed (in case of failures close() might not be called)
            try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness2 = createTestHarness(topicUri)) {
                testHarness2.setup();
                // restore from snapshot1, transactions with records 43 and 44 should be aborted
                testHarness2.initializeState(snapshot);
                testHarness2.open();
            }

            assertExactlyOnceForTopicUri(createProperties(), topicUri, 0, Arrays.asList(42));
            deleteTestTopic(topic);
        } catch (Exception ex) {
            // testHarness1 will be fenced off after creating and closing testHarness2
            if (!findSerializedThrowable(ex, ProducerFencedException.class, ClassLoader.getSystemClassLoader()).isPresent()) {
                throw ex;
            }
        }
        checkProducerLeak();
    }

    @Test
    public void testFlinkPscProducerFailBeforeNotify() throws Exception {
        String topic = "flink-kafka-producer-fail-before-notify";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topicUri);

        testHarness.setup();
        testHarness.open();
        testHarness.processElement(42, 0);
        testHarness.snapshot(0, 1);
        testHarness.processElement(43, 2);
        OperatorSubtaskState snapshot = testHarness.snapshot(1, 3);

        int leaderId = pscTestEnvWithKafka.getLeaderToShutDown(topic);
        failBroker(leaderId);

        try {
            testHarness.processElement(44, 4);
            testHarness.snapshot(2, 5);
            fail();
        } catch (Exception ex) {
            // expected
        }
        try {
            testHarness.close();
        } catch (Exception ex) {
        }

        pscTestEnvWithKafka.restartBroker(leaderId);

        testHarness = createTestHarness(topicUri);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.close();

        assertExactlyOnceForTopicUri(createProperties(), topicUri, 0, Arrays.asList(42, 43));

        Thread.sleep(5000); // wait for broker to start up before deleting topic
        deleteTestTopic(topic);
        checkProducerLeak();
    }

    /**
     * This tests checks whether FlinkPscProducer correctly aborts lingering transactions after a failure.
     * If such transactions were left alone lingering it consumers would be unable to read committed records
     * that were created after this lingering transaction.
     */
    @Test
    public void testFailBeforeNotifyAndResumeWorkAfterwards() throws Exception {
        String topic = "flink-kafka-producer-fail-before-notify";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        OneInputStreamOperatorTestHarness<Integer, Object> testHarness1 = createTestHarness(topicUri);
        checkProducerLeak();
        testHarness1.setup();
        testHarness1.open();
        testHarness1.processElement(42, 0);
        testHarness1.snapshot(0, 1);
        testHarness1.processElement(43, 2);
        OperatorSubtaskState snapshot1 = testHarness1.snapshot(1, 3);

        testHarness1.processElement(44, 4);
        testHarness1.snapshot(2, 5);
        testHarness1.processElement(45, 6);

        // do not close previous testHarness to make sure that closing do not clean up something (in case of failure
        // there might not be any close)
        OneInputStreamOperatorTestHarness<Integer, Object> testHarness2 = createTestHarness(topicUri);
        testHarness2.setup();
        // restore from snapshot1, transactions with records 44 and 45 should be aborted
        testHarness2.initializeState(snapshot1);
        testHarness2.open();

        // write and commit more records, after potentially lingering transactions
        testHarness2.processElement(46, 7);
        testHarness2.snapshot(4, 8);
        testHarness2.processElement(47, 9);
        testHarness2.notifyOfCompletedCheckpoint(4);

        //now we should have:
        // - records 42 and 43 in committed transactions
        // - aborted transactions with records 44 and 45
        // - committed transaction with record 46
        // - pending transaction with record 47
        assertExactlyOnceForTopicUri(createProperties(), topicUri, 0, Arrays.asList(42, 43, 46));

        try {
            testHarness1.close();
        } catch (Exception e) {
            // The only acceptable root exception is ProducerFencedException because testHarness2 uses the same
            // transactional ID.
            if (!isCausedBy(e, ProducerFencedException.class))
                fail("Received unexpected exception " + e);
        }
        testHarness2.close();
        deleteTestTopic(topic);
        checkProducerLeak();
    }

    @Test
    public void testFailAndRecoverSameCheckpointTwice() throws Exception {
        String topic = "flink-kafka-producer-fail-and-recover-same-checkpoint-twice";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        OperatorSubtaskState snapshot1;
        try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topicUri)) {
            testHarness.setup();
            testHarness.open();
            testHarness.processElement(42, 0);
            testHarness.snapshot(0, 1);
            testHarness.processElement(43, 2);
            snapshot1 = testHarness.snapshot(1, 3);

            testHarness.processElement(44, 4);
        }

        try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topicUri)) {
            testHarness.setup();
            // restore from snapshot1, transactions with records 44 and 45 should be aborted
            testHarness.initializeState(snapshot1);
            testHarness.open();

            // write and commit more records, after potentially lingering transactions
            testHarness.processElement(44, 7);
            testHarness.snapshot(2, 8);
            testHarness.processElement(45, 9);
        }

        try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topicUri)) {
            testHarness.setup();
            // restore from snapshot1, transactions with records 44 and 45 should be aborted
            testHarness.initializeState(snapshot1);
            testHarness.open();

            // write and commit more records, after potentially lingering transactions
            testHarness.processElement(44, 7);
            testHarness.snapshot(3, 8);
            testHarness.processElement(45, 9);
        }

        //now we should have:
        // - records 42 and 43 in committed transactions
        // - aborted transactions with records 44 and 45
        assertExactlyOnceForTopicUri(createProperties(), topicUri, 0, Arrays.asList(42, 43));
        deleteTestTopic(topic);
        checkProducerLeak();
    }

    /**
     * This tests checks whether FlinkPscProducer correctly aborts lingering transactions after a failure,
     * which happened before first checkpoint and was followed up by reducing the parallelism.
     * If such transactions were left alone lingering it consumers would be unable to read committed records
     * that were created after this lingering transaction.
     */
    @Test
    public void testScaleDownBeforeFirstCheckpoint() throws Exception {
        String topic = "scale-down-before-first-checkpoint";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        List<AutoCloseable> operatorsToClose = new ArrayList<>();
        int preScaleDownParallelism = Math.max(2, FlinkPscProducer.SAFE_SCALE_DOWN_FACTOR);
        for (int subtaskIndex = 0; subtaskIndex < preScaleDownParallelism; subtaskIndex++) {
            OneInputStreamOperatorTestHarness<Integer, Object> preScaleDownOperator = createTestHarness(
                    topicUri,
                    preScaleDownParallelism,
                    preScaleDownParallelism,
                    subtaskIndex,
                    FlinkPscProducer.Semantic.EXACTLY_ONCE);

            preScaleDownOperator.setup();
            preScaleDownOperator.open();
            preScaleDownOperator.processElement(subtaskIndex * 2, 0);
            preScaleDownOperator.snapshot(0, 1);
            preScaleDownOperator.processElement(subtaskIndex * 2 + 1, 2);

            operatorsToClose.add(preScaleDownOperator);
        }

        // do not close previous testHarnesses to make sure that closing do not clean up something (in case of failure
        // there might not be any close)

        // After previous failure simulate restarting application with smaller parallelism
        OneInputStreamOperatorTestHarness<Integer, Object> postScaleDownOperator1 = createTestHarness(topicUri, 1, 1, 0, FlinkPscProducer.Semantic.EXACTLY_ONCE);

        postScaleDownOperator1.setup();
        postScaleDownOperator1.open();

        // write and commit more records, after potentially lingering transactions
        postScaleDownOperator1.processElement(46, 7);
        postScaleDownOperator1.snapshot(4, 8);
        postScaleDownOperator1.processElement(47, 9);
        postScaleDownOperator1.notifyOfCompletedCheckpoint(4);

        //now we should have:
        // - records 42, 43, 44 and 45 in aborted transactions
        // - committed transaction with record 46
        // - pending transaction with record 47
        //TODO: Investigate why a higher timeout (120 secs instead of 30) is required for this to succeed
        assertExactlyOnceForTopicUri(createProperties(), topicUri, 0, Arrays.asList(46), 120_000);

        postScaleDownOperator1.close();
        // ignore ProducerFencedExceptions, because postScaleDownOperator1 could reuse transactional ids.
        for (AutoCloseable operatorToClose : operatorsToClose) {
            closeIgnoringProducerFenced(operatorToClose);
        }
        deleteTestTopic(topic);
        checkProducerLeak();
    }

    /**
     * Each instance of FlinkPscProducer uses it's own pool of transactional ids. After the restore from checkpoint
     * transactional ids are redistributed across the subtasks. In case of scale down, the surplus transactional ids
     * are dropped. In case of scale up, new one are generated (for the new subtasks). This test make sure that sequence
     * of scaling down and up again works fine. Especially it checks whether the newly generated ids in scaling up
     * do not overlap with ids that were used before scaling down. For example we start with 4 ids and parallelism 4:
     * [1], [2], [3], [4] - one assigned per each subtask
     * we scale down to parallelism 2:
     * [1, 2], [3, 4] - first subtask got id 1 and 2, second got ids 3 and 4
     * surplus ids are dropped from the pools and we scale up to parallelism 3:
     * [1 or 2], [3 or 4], [???]
     * new subtask have to generate new id(s), but he can not use ids that are potentially in use, so it has to generate
     * new ones that are greater then 4.
     */
    @Test
    public void testScaleUpAfterScalingDown() throws Exception {
        String topic = "scale-up-after-scaling-down";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        final int parallelism1 = 4;
        final int parallelism2 = 2;
        final int parallelism3 = 3;
        final int maxParallelism = Math.max(parallelism1, Math.max(parallelism2, parallelism3));

        OperatorSubtaskState operatorSubtaskState = repartitionAndExecute(
                topicUri,
                OperatorSubtaskState.builder().build(),
                parallelism1,
                parallelism1,
                maxParallelism,
                IntStream.range(0, parallelism1).boxed().iterator());

        operatorSubtaskState = repartitionAndExecute(
                topicUri,
                operatorSubtaskState,
                parallelism1,
                parallelism2,
                maxParallelism,
                IntStream.range(parallelism1, parallelism1 + parallelism2).boxed().iterator());

        operatorSubtaskState = repartitionAndExecute(
                topicUri,
                operatorSubtaskState,
                parallelism2,
                parallelism3,
                maxParallelism,
                IntStream.range(parallelism1 + parallelism2, parallelism1 + parallelism2 + parallelism3).boxed().iterator());

        // After each previous repartitionAndExecute call, we are left with some lingering transactions, that would
        // not allow us to read all committed messages from the topic. Thus we initialize operators from
        // OperatorSubtaskState once more, but without any new data. This should terminate all ongoing transactions.

        repartitionAndExecute(
                topicUri,
                operatorSubtaskState,
                parallelism3,
                1,
                maxParallelism,
                Collections.emptyIterator());

        assertExactlyOnceForTopicUri(
                createProperties(),
                topicUri,
                0,
                IntStream.range(0, parallelism1 + parallelism2 + parallelism3).boxed().collect(Collectors.toList()));
        deleteTestTopic(topic);
        checkProducerLeak();
    }

    private OperatorSubtaskState repartitionAndExecute(
            String topicUri,
            OperatorSubtaskState inputStates,
            int oldParallelism,
            int newParallelism,
            int maxParallelism,
            Iterator<Integer> inputData) throws Exception {

        List<OperatorSubtaskState> outputStates = new ArrayList<>();
        List<OneInputStreamOperatorTestHarness<Integer, Object>> testHarnesses = new ArrayList<>();

        for (int subtaskIndex = 0; subtaskIndex < newParallelism; subtaskIndex++) {
            OperatorSubtaskState initState = AbstractStreamOperatorTestHarness.repartitionOperatorState(
                    inputStates, maxParallelism, oldParallelism, newParallelism, subtaskIndex);

            OneInputStreamOperatorTestHarness<Integer, Object> testHarness =
                    createTestHarness(topicUri, maxParallelism, newParallelism, subtaskIndex, FlinkPscProducer.Semantic.EXACTLY_ONCE);
            testHarnesses.add(testHarness);

            testHarness.setup();

            testHarness.initializeState(initState);
            testHarness.open();

            if (inputData.hasNext()) {
                int nextValue = inputData.next();
                testHarness.processElement(nextValue, 0);
                OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

                outputStates.add(snapshot);
                checkState(snapshot.getRawOperatorState().isEmpty(), "Unexpected raw operator state");
                checkState(snapshot.getManagedKeyedState().isEmpty(), "Unexpected managed keyed state");
                checkState(snapshot.getRawKeyedState().isEmpty(), "Unexpected raw keyed state");

                for (int i = 1; i < FlinkPscProducer.DEFAULT_PSC_PRODUCERS_POOL_SIZE - 1; i++) {
                    testHarness.processElement(-nextValue, 0);
                    testHarness.snapshot(i, 0);
                }
            }
        }

        for (OneInputStreamOperatorTestHarness<Integer, Object> testHarness : testHarnesses) {
            testHarness.close();
        }

        return AbstractStreamOperatorTestHarness.repackageState(
                outputStates.toArray(new OperatorSubtaskState[outputStates.size()]));
    }

    @Test
    public void testRecoverCommittedTransaction() throws Exception {
        String topic = "flink-kafka-producer-recover-committed-transaction";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topicUri);

        testHarness.setup();
        testHarness.open(); // producerA - start transaction (txn) 0
        testHarness.processElement(42, 0); // producerA - write 42 in txn 0
        OperatorSubtaskState checkpoint0 = testHarness.snapshot(0, 1); // producerA - pre commit txn 0, producerB - start txn 1
        testHarness.processElement(43, 2); // producerB - write 43 in txn 1
        testHarness.notifyOfCompletedCheckpoint(0); // producerA - commit txn 0 and return to the pool
        testHarness.snapshot(1, 3); // producerB - pre txn 1,  producerA - start txn 2
        testHarness.processElement(44, 4); // producerA - write 44 in txn 2
        testHarness.close(); // producerA - abort txn 2

        testHarness = createTestHarness(topicUri);
        testHarness.initializeState(checkpoint0); // recover state 0 - producerA recover and commit txn 0
        testHarness.close();

        assertExactlyOnceForTopicUri(createProperties(), topicUri, 0, Arrays.asList(42));

        deleteTestTopic(topic);
        checkProducerLeak();
    }

    @Test
    public void testRunOutOfProducersInThePool() throws Exception {
        String topic = "flink-kafka-run-out-of-producers";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;

        try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topicUri)) {

            testHarness.setup();
            testHarness.open();

            for (int i = 0; i < FlinkPscProducer.DEFAULT_PSC_PRODUCERS_POOL_SIZE * 2; i++) {
                testHarness.processElement(i, i * 2);
                testHarness.snapshot(i, i * 2 + 1);
            }
        } catch (Exception ex) {
            if (!ex.getCause().getMessage().startsWith("Too many ongoing")) {
                throw ex;
            }
        }
        deleteTestTopic(topic);
        checkProducerLeak();
    }

    @Test
    public void testMigrateFromAtLeastOnceToExactlyOnce() throws Exception {
        String topic = "testMigrateFromAtLeastOnceToExactlyOnce";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        testRecoverWithChangeSemantics(topicUri, FlinkPscProducer.Semantic.AT_LEAST_ONCE, FlinkPscProducer.Semantic.EXACTLY_ONCE);
        assertExactlyOnceForTopicUri(createProperties(), topicUri, 0, Arrays.asList(42, 43, 44, 45));
        deleteTestTopic(topic);
    }

    @Test
    public void testMigrateFromAtExactlyOnceToAtLeastOnce() throws Exception {
        String topic = "testMigrateFromExactlyOnceToAtLeastOnce";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        testRecoverWithChangeSemantics(topicUri, FlinkPscProducer.Semantic.EXACTLY_ONCE, FlinkPscProducer.Semantic.AT_LEAST_ONCE);
        assertExactlyOnceForTopicUri(createProperties(), topicUri, 0, Arrays.asList(42, 43, 45, 46, 47));
        deleteTestTopic(topic);
    }

    private void testRecoverWithChangeSemantics(
            String topicUri,
            FlinkPscProducer.Semantic fromSemantic,
            FlinkPscProducer.Semantic toSemantic) throws Exception {
        OperatorSubtaskState producerSnapshot;
        try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topicUri, fromSemantic)) {
            testHarness.setup();
            testHarness.open();
            testHarness.processElement(42, 0);
            testHarness.snapshot(0, 1);
            testHarness.processElement(43, 2);
            testHarness.notifyOfCompletedCheckpoint(0);
            producerSnapshot = testHarness.snapshot(1, 3);
            testHarness.processElement(44, 4);
        }

        try (OneInputStreamOperatorTestHarness<Integer, Object> testHarness = createTestHarness(topicUri, toSemantic)) {
            testHarness.setup();
            testHarness.initializeState(producerSnapshot);
            testHarness.open();
            testHarness.processElement(45, 7);
            testHarness.snapshot(2, 8);
            testHarness.processElement(46, 9);
            testHarness.notifyOfCompletedCheckpoint(2);
            testHarness.processElement(47, 9);
        }
        checkProducerLeak();
    }

    // -----------------------------------------------------------------------------------------------------------------

    // shut down a Kafka broker
    private void failBroker(int brokerId) throws Exception {
        pscTestEnvWithKafka.stopBroker(brokerId);
    }

    private void closeIgnoringProducerFenced(AutoCloseable autoCloseable) throws Exception {
        try {
            autoCloseable.close();
        } catch (Exception ex) {
            if (!isCausedBy(ex, ProducerFencedException.class)) {
                throw ex;
            }
        }
    }

    private OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness(String topicUri) throws Exception {
        return createTestHarness(topicUri, FlinkPscProducer.Semantic.EXACTLY_ONCE);
    }

    private OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness(
            String topicUri,
            FlinkPscProducer.Semantic semantic) throws Exception {
        return createTestHarness(topicUri, 1, 1, 0, semantic);
    }

    private OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness(
            String topicUri,
            int maxParallelism,
            int parallelism,
            int subtaskIndex,
            FlinkPscProducer.Semantic semantic) throws Exception {

        FlinkPscProducer<Integer> flinkPscProducer = new FlinkPscProducer<>(
                topicUri,
                integerKeyedSerializationSchema,
                createProperties(),
                semantic);

        return new OneInputStreamOperatorTestHarness<>(
                new StreamSink<>(flinkPscProducer),
                maxParallelism,
                parallelism,
                subtaskIndex,
                IntSerializer.INSTANCE,
                new OperatorID(42, 44));
    }

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.putAll(standardPscConsumerConfiguration);
        properties.putAll(securePscConsumerConfiguration);
        properties.putAll(standardPscProducerConfiguration);
        properties.putAll(securePscConsumerConfiguration);
        properties.putAll(pscDiscoveryConfiguration);
        properties.put(FlinkPscProducer.KEY_DISABLE_METRICS, "true");
        return properties;
    }

    private boolean isCausedBy(FlinkPscErrorCode expectedErrorCode, Throwable ex) {
        Optional<FlinkPscException> cause = findSerializedThrowable(ex, FlinkPscException.class, ClassLoader.getSystemClassLoader());
        if (cause.isPresent()) {
            return cause.get().getErrorCode().equals(expectedErrorCode);
        }
        return false;
    }

    private boolean isCausedBy(Throwable exception, Class exceptionClass) {
        while (exception.getCause() != null) {
            if (exceptionClass.isInstance(exception.getCause()))
                return true;
            exception = exception.getCause();
        }
        return false;
    }

    private void checkProducerLeak() {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("kafka-producer-network-thread")) {
                fail("Detected producer leak. Thread name: " + t.getName());
            }
        }
    }

}
