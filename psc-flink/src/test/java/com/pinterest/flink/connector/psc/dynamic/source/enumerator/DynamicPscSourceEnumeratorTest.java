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

package com.pinterest.flink.connector.psc.dynamic.source.enumerator;

import com.google.common.collect.ImmutableSet;
import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.dynamic.source.DynamicPscSourceOptions;
import com.pinterest.flink.connector.psc.dynamic.source.GetMetadataUpdateEvent;
import com.pinterest.flink.connector.psc.dynamic.source.enumerator.subscriber.PscStreamSetSubscriber;
import com.pinterest.flink.connector.psc.dynamic.source.split.DynamicPscSourceSplit;
import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.testutils.MockPscMetadataService;
import com.pinterest.flink.streaming.connectors.psc.DynamicPscSourceTestHelperWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.exception.PscException;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.mock.Whitebox;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A test for {@link DynamicPscSourceEnumerator}. */
public class DynamicPscSourceEnumeratorTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String TOPIC = "DynamicPscSourceEnumeratorTest";
    private static final int NUM_SPLITS_PER_CLUSTER = 3;
    private static final int NUM_RECORDS_PER_SPLIT = 5;

    @BeforeAll
    public static void beforeAll() throws Throwable {
        DynamicPscSourceTestHelperWithKafkaAsPubSub.setup();
        DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(TOPIC, NUM_SPLITS_PER_CLUSTER, 1);
        DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                TOPIC, NUM_SPLITS_PER_CLUSTER, NUM_RECORDS_PER_SPLIT);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        DynamicPscSourceTestHelperWithKafkaAsPubSub.tearDown();
    }

    @Test
    public void testStartupWithoutContinuousDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
        }
    }

    @Test
    public void testStartupWithContinuousDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             DynamicPscSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                (properties) ->
                                        properties.setProperty(
                                                DynamicPscSourceOptions
                                                        .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                        .key(),
                                                "1"))) {
            enumerator.start();

            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat(context.getPeriodicCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
        }
    }

    @Test
    public void testStartupWithPscMetadataServiceFailure_noPeriodicDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             DynamicPscSourceEnumerator enumerator =
                        createEnumerator(
                                context, new MockPscMetadataService(true), (properties) -> {})) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
            assertThatThrownBy(() -> runAllOneTimeCallables(context))
                    .as(
                            "Exception expected since periodic discovery is disabled and metadata is required for setting up the job")
                    .hasRootCause(new RuntimeException("Mock exception"));
        }
    }

    @Test
    public void testStartupWithPscMetadataServiceFailure_withContinuousDiscovery()
            throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             DynamicPscSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                new MockPscMetadataService(true),
                                (properties) ->
                                        properties.setProperty(
                                                DynamicPscSourceOptions
                                                        .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                        .key(),
                                                "1"))) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).hasSize(1);
            assertThatThrownBy(() -> context.runPeriodicCallable(0))
                    .as("Exception expected since there is no state")
                    .hasRootCause(new RuntimeException("Mock exception"));
        }
    }

    @Test
    public void
    testStartupWithPscMetadataServiceFailure_withContinuousDiscoveryAndCheckpointState()
                    throws Throwable {
        // init enumerator with checkpoint state
        final DynamicPscSourceEnumState dynamicKafkaSourceEnumState = getCheckpointState();
        Properties properties = new Properties();
        properties.setProperty(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        new DynamicPscSourceEnumerator(
                                new PscStreamSetSubscriber(Collections.singleton(TOPIC)),
                                new MockPscMetadataService(true),
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                dynamicKafkaSourceEnumState,
                                new TestPscEnumContextProxyFactory())) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).hasSize(1);
            // no exception
            context.runPeriodicCallable(0);

            assertThatThrownBy(() -> context.runPeriodicCallable(0))
                    .hasRootCause(new RuntimeException("Mock exception"));
        }
    }

    @Test
    public void testHandleMetadataServiceError() throws Throwable {
        int failureThreshold = 5;

        Properties properties = new Properties();
        properties.setProperty(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                Integer.toString(failureThreshold));
        properties.setProperty(PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");

        MockPscMetadataService mockPscMetadataService =
                new MockPscMetadataService(
                        Collections.singleton(DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC)));

        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        new DynamicPscSourceEnumerator(
                                new PscStreamSetSubscriber(Collections.singleton(TOPIC)),
                                mockPscMetadataService,
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new DynamicPscSourceEnumState(),
                                new TestPscEnumContextProxyFactory())) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).hasSize(1);
            context.runPeriodicCallable(0);

            // init splits
            runAllOneTimeCallables(context);

            // swap to exceptional metadata service
            mockPscMetadataService.setThrowException(true);

            for (int i = 0; i < failureThreshold; i++) {
                context.runPeriodicCallable(0);
            }

            for (int i = 0; i < 2; i++) {
                assertThatThrownBy(() -> context.runPeriodicCallable(0))
                        .hasRootCause(new RuntimeException("Mock exception"));
                // Need to reset internal throwable reference after each invocation of
                // runPeriodicCallable,
                // since the context caches the previous exceptions indefinitely
                AtomicReference<Throwable> errorInWorkerThread =
                        (AtomicReference<Throwable>)
                                Whitebox.getInternalState(context, "errorInWorkerThread");
                errorInWorkerThread.set(null);
            }

            mockPscMetadataService.setThrowException(false);
            assertThatCode(() -> context.runPeriodicCallable(0))
                    .as("Exception counter should have been reset")
                    .doesNotThrowAnyException();
        }
    }

    @Test
    public void testPscMetadataServiceDiscovery() throws Throwable {
        PscStream pscStreamWithOneCluster = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC);
        pscStreamWithOneCluster
                .getClusterMetadataMap()
                .remove(DynamicPscSourceTestHelperWithKafkaAsPubSub.getPubSubClusterId(1));

        PscStream pscStreamWithTwoClusters = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC);

        MockPscMetadataService mockPscMetadataService =
                new MockPscMetadataService(Collections.singleton(pscStreamWithOneCluster));

        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                mockPscMetadataService,
                                (properties) ->
                                        properties.setProperty(
                                                DynamicPscSourceOptions
                                                        .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                        .key(),
                                                "1"))) {
            enumerator.start();

            context.runPeriodicCallable(0);

            // 1 callable for main enumerator and 2 for the sub enumerators since we have 2 clusters
            runAllOneTimeCallables(context);

            assertThat(context.getOneTimeCallables())
                    .as("There should be no more callables after running the 4")
                    .isEmpty();

            assertThat(context.getSplitsAssignmentSequence())
                    .as("no splits should be assigned yet since there are no readers")
                    .isEmpty();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(), pscStreamWithOneCluster);
            int currentNumSplits = context.getSplitsAssignmentSequence().size();

            // no changes to splits
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);
            assertThat(context.getSplitsAssignmentSequence()).hasSize(currentNumSplits);

            // mock metadata change
            mockPscMetadataService.setPscStreams(
                    Collections.singleton(pscStreamWithTwoClusters));

            // changes should have occurred here
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);
            assertThat(context.getSplitsAssignmentSequence())
                    .as(
                            "1 additional split assignment since there was 1 metadata update that caused a change")
                    .hasSize(currentNumSplits + 1);
            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(), pscStreamWithTwoClusters);
        }
    }

    @Test
    public void testReaderRegistrationAfterSplitDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            runAllOneTimeCallables(context);

            assertThat(context.getOneTimeCallables())
                    .as("There should be no more callables after running the 4")
                    .isEmpty();
            assertThat(context.getSplitsAssignmentSequence())
                    .as("no splits should be assigned yet since there are no readers")
                    .isEmpty();
            assertThat(context.getSplitsAssignmentSequence())
                    .as("no readers have registered yet")
                    .isEmpty();
            assertThat(context.getSentSourceEvent()).as("no readers have registered yet").isEmpty();

            // initialize readers 0 and 2
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            assertThat(context.getSentSourceEvent().keySet())
                    .as("reader 0 and 2 should have only received the source event")
                    .containsExactlyInAnyOrder(0, 2);
            Set<Integer> allReadersThatReceivedSplits =
                    context.getSplitsAssignmentSequence().stream()
                            .flatMap(
                                    splitAssignment ->
                                            splitAssignment.assignment().keySet().stream())
                            .collect(Collectors.toSet());
            assertThat(allReadersThatReceivedSplits)
                    .as("reader 0 and 2 should hve only received splits")
                    .containsExactlyInAnyOrder(0, 2);

            // initialize readers 1
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            assertThat(context.getSentSourceEvent().keySet())
                    .as("all readers should have received get metadata update event")
                    .containsExactlyInAnyOrder(0, 1, 2);

            for (List<SourceEvent> sourceEventsPerReader : context.getSentSourceEvent().values()) {
                assertThat(sourceEventsPerReader)
                        .as("there should have been only 1 source event per reader")
                        .hasSize(1);
            }

            // should have all splits assigned by now
            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC));
        }
    }

    @Test
    public void testReaderRegistrationBeforeSplitDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            assertThat(context.getSplitsAssignmentSequence())
                    .as("readers should not be assigned yet since there are no splits")
                    .isEmpty();

            // 1 callable for main enumerator and 3 for the sub enumerators since we have 3 clusters
            runAllOneTimeCallables(context);

            assertThat(context.getOneTimeCallables())
                    .as("There should be no more callables after running the 4")
                    .isEmpty();

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC));
        }
    }

    @Test
    public void testSnapshotState() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            DynamicPscSourceEnumState stateBeforeSplitAssignment = enumerator.snapshotState(-1);
            assertThat(
                            stateBeforeSplitAssignment.getClusterEnumeratorStates().values()
                                    .stream()
                                    .map(subState -> subState.assignedPartitions().stream())
                                    .count())
                    .as("no readers registered, so state should be empty")
                    .isZero();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            assertThat(context.getSplitsAssignmentSequence())
                    .as("readers should not be assigned yet since there are no splits")
                    .isEmpty();

            // 1 callable for main enumerator and 3 for the sub enumerators since we have 3 clusters
            runAllOneTimeCallables(context);

            assertThat(context.getOneTimeCallables())
                    .as("There should be no more callables after running the 4")
                    .isEmpty();

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC));

            DynamicPscSourceEnumState stateAfterSplitAssignment = enumerator.snapshotState(-1);

            assertThat(
                            stateAfterSplitAssignment.getClusterEnumeratorStates().values().stream()
                                    .flatMap(enumState -> enumState.assignedPartitions().stream())
                                    .count())
                    .isEqualTo(
                            NUM_SPLITS_PER_CLUSTER
                                    * DynamicPscSourceTestHelperWithKafkaAsPubSub.NUM_PUBSUB_CLUSTERS);
        }
    }

    @Test
    public void testStartupWithCheckpointState() throws Throwable {
        // init enumerator with checkpoint state
        final DynamicPscSourceEnumState dynamicPscSourceEnumState = getCheckpointState();
        Properties properties = new Properties();
        properties.setProperty(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        new DynamicPscSourceEnumerator(
                                new PscStreamSetSubscriber(Collections.singleton(TOPIC)),
                                new MockPscMetadataService(
                                        Collections.singleton(
                                                DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(
                                                        TOPIC))),
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                dynamicPscSourceEnumState,
                                new TestPscEnumContextProxyFactory())) {
            // start and check callables
            enumerator.start();
            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as(
                            "3 one time callables should have been scheduled. 1 for main enumerator and then 2 for each underlying enumerator")
                    .hasSize(1 + DynamicPscSourceTestHelperWithKafkaAsPubSub.NUM_PUBSUB_CLUSTERS);

            // initialize all readers and do split assignment
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);
            runAllOneTimeCallables(context);

            assertThat(context.getSentSourceEvent()).as("3 readers registered").hasSize(3);
            for (List<SourceEvent> sourceEventsReceived : context.getSentSourceEvent().values()) {
                assertThat(sourceEventsReceived)
                        .as("each reader should have sent 1 source event")
                        .hasSize(1);
            }

            assertThat(context.getSplitsAssignmentSequence())
                    .as(
                            "there should not be new splits and we don't assign previously assigned splits at startup and there is no metadata/split changes")
                    .isEmpty();
        }

        // test with periodic discovery enabled
        properties.setProperty(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        new DynamicPscSourceEnumerator(
                                new PscStreamSetSubscriber(Collections.singleton(TOPIC)),
                                new MockPscMetadataService(
                                        Collections.singleton(
                                                DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(
                                                        TOPIC))),
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                dynamicPscSourceEnumState,
                                new TestPscEnumContextProxyFactory())) {
            enumerator.start();
            assertThat(context.getPeriodicCallables())
                    .as("A periodic time partition discovery callable should have been scheduled")
                    .hasSize(1);
            assertThat(context.getOneTimeCallables())
                    .as(
                            "0 one time callables for main enumerator and 2 one time callables for each underlying enumerator should have been scheduled")
                    .hasSize(DynamicPscSourceTestHelperWithKafkaAsPubSub.NUM_PUBSUB_CLUSTERS);

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            // checkpoint state should have triggered split assignment
            assertThat(context.getSplitsAssignmentSequence())
                    .as(
                            "There is no split assignment since there are no new splits that are not contained in state")
                    .isEmpty();
        }
    }

    @Test
    public void testAddSplitsBack() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator = createEnumerator(context)) {
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);
            enumerator.start();

            runAllOneTimeCallables(context);

            Map<Integer, Set<DynamicPscSourceSplit>> readerAssignmentsBeforeFailure =
                    getReaderAssignments(context);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("we only expect splits have been assigned 2 times")
                    .hasSize(DynamicPscSourceTestHelperWithKafkaAsPubSub.NUM_PUBSUB_CLUSTERS);

            // simulate failures
            context.unregisterReader(0);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(0), 0);
            context.unregisterReader(2);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(2), 2);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("Splits assignment should be unchanged")
                    .hasSize(DynamicPscSourceTestHelperWithKafkaAsPubSub.NUM_PUBSUB_CLUSTERS);

            // mock reader recovery
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC));
            assertThat(getReaderAssignments(context))
                    .containsAllEntriesOf(readerAssignmentsBeforeFailure);
            assertThat(context.getSplitsAssignmentSequence())
                    .as(
                            "the readers came back up, so there should be 2 additional split assignments in the sequence")
                    .hasSize(DynamicPscSourceTestHelperWithKafkaAsPubSub.NUM_PUBSUB_CLUSTERS + 2);
        }
    }

    @Test
    public void testEnumeratorDoesNotAssignDuplicateSplitsInMetadataUpdate() throws Throwable {
        PscStream pscStreamWithOneCluster = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC);
        pscStreamWithOneCluster
                .getClusterMetadataMap()
                .remove(DynamicPscSourceTestHelperWithKafkaAsPubSub.getPubSubClusterId(1));

        PscStream pscStreamWithTwoClusters = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC);

        MockPscMetadataService mockPscMetadataService =
                new MockPscMetadataService(Collections.singleton(pscStreamWithOneCluster));

        Properties properties = new Properties();
        properties.setProperty(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");

        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        new DynamicPscSourceEnumerator(
                                new PscStreamSetSubscriber(Collections.singleton(TOPIC)),
                                mockPscMetadataService,
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new DynamicPscSourceEnumState(),
                                new TestPscEnumContextProxyFactory())) {

            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);
            enumerator.start();

            // run all discovery
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(), pscStreamWithOneCluster);

            // trigger metadata change
            mockPscMetadataService.setPscStreams(
                    Collections.singleton(pscStreamWithTwoClusters));
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(), pscStreamWithTwoClusters);

            Map<String, Integer> splitAssignmentFrequencyMap = new HashMap<>();
            for (SplitsAssignment<DynamicPscSourceSplit> splitsAssignmentStep :
                    context.getSplitsAssignmentSequence()) {
                for (List<DynamicPscSourceSplit> assignments :
                        splitsAssignmentStep.assignment().values()) {
                    for (DynamicPscSourceSplit assignment : assignments) {
                        splitAssignmentFrequencyMap.put(
                                assignment.splitId(),
                                splitAssignmentFrequencyMap.getOrDefault(assignment.splitId(), 0)
                                        + 1);
                    }
                }
            }

            assertThat(splitAssignmentFrequencyMap.values())
                    .as("all splits should have been assigned once")
                    .allMatch(count -> count == 1);
        }
    }

    @Test
    public void testInitExceptionNonexistingPubSubCluster() {
        Properties fakeProperties = new Properties();
        fakeProperties.setProperty(
                PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        MockPscMetadataService mockPscMetadataServiceWithUnavailableCluster =
                new MockPscMetadataService(
                        ImmutableSet.of(
                                DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC),
                                new PscStream(
                                        "fake-stream",
                                        Collections.singletonMap(
                                                "fake-cluster",
                                                new ClusterMetadata(
                                                        Collections.singleton("fake-topic"),
                                                        fakeProperties)))));
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        createEnumerator(context, mockPscMetadataServiceWithUnavailableCluster)) {
            enumerator.start();

            runAllOneTimeCallables(context);
        } catch (Throwable throwable) {
            assertThat(throwable).hasRootCauseInstanceOf(PscException.class);
        }
    }

    @Test
    public void testEnumeratorErrorPropagation() {
        Properties fakeProperties = new Properties();
        fakeProperties.setProperty(
                PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        PscStream fakeStream =
                new PscStream(
                        "fake-stream",
                        Collections.singletonMap(
                                "fake-cluster",
                                new ClusterMetadata(
                                        Collections.singleton("fake-topic"), fakeProperties)));

        MockPscMetadataService mockPscMetadataServiceWithUnavailableCluster =
                new MockPscMetadataService(
                        ImmutableSet.of(
                                DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC), fakeStream));
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        createEnumerator(context, mockPscMetadataServiceWithUnavailableCluster)) {
            enumerator.start();

            runAllOneTimeCallables(context);
        } catch (Throwable throwable) {
            assertThat(throwable).hasRootCauseInstanceOf(PscException.class);
        }
    }

    private DynamicPscSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicPscSourceSplit> context) {
        return createEnumerator(
                context,
                new MockPscMetadataService(
                        Collections.singleton(DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC))),
                (properties) -> {});
    }

    private DynamicPscSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicPscSourceSplit> context,
            PscMetadataService pscMetadataService) {
        return createEnumerator(context, pscMetadataService, (properties) -> {});
    }

    private DynamicPscSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicPscSourceSplit> context,
            Consumer<Properties> applyPropertiesConsumer) {
        return createEnumerator(
                context,
                new MockPscMetadataService(
                        Collections.singleton(DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC))),
                applyPropertiesConsumer);
    }

    private DynamicPscSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicPscSourceSplit> context,
            PscMetadataService kafkaMetadataService,
            Consumer<Properties> applyPropertiesConsumer) {
        Properties properties = new Properties();
        applyPropertiesConsumer.accept(properties);
        properties.putIfAbsent(PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.putIfAbsent(
                DynamicPscSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
        return new DynamicPscSourceEnumerator(
                new PscStreamSetSubscriber(Collections.singleton(TOPIC)),
                kafkaMetadataService,
                context,
                OffsetsInitializer.earliest(),
                new NoStoppingOffsetsInitializer(),
                properties,
                Boundedness.CONTINUOUS_UNBOUNDED,
                new DynamicPscSourceEnumState(),
                new TestPscEnumContextProxyFactory());
    }

    private void mockRegisterReaderAndSendReaderStartupEvent(
            MockSplitEnumeratorContext<DynamicPscSourceSplit> context,
            DynamicPscSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location " + reader));
        enumerator.addReader(reader);
        // readers send source event at startup
        enumerator.handleSourceEvent(reader, new GetMetadataUpdateEvent());
    }

    private void verifyAllSplitsHaveBeenAssigned(
            List<SplitsAssignment<DynamicPscSourceSplit>> splitsAssignmentSequence,
            PscStream kafkaStream) {
        Map<String, Set<String>> clusterTopicMap = new HashMap<>();
        for (Entry<String, ClusterMetadata> entry :
                kafkaStream.getClusterMetadataMap().entrySet()) {
            clusterTopicMap
                    .computeIfAbsent(entry.getKey(), unused -> new HashSet<>())
                    .addAll(entry.getValue().getTopics());
        }

        Set<DynamicPscSourceSplit> splitsAssigned =
                splitsAssignmentSequence.stream()
                        .flatMap(
                                splitsAssignment ->
                                        splitsAssignment.assignment().values().stream()
                                                .flatMap(Collection::stream))
                        .collect(Collectors.toSet());

        assertThat(splitsAssignmentSequence).isNotEmpty();

        Map<String, Set<TopicUriPartition>> clusterToTopicPartition = new HashMap<>();
        for (SplitsAssignment<DynamicPscSourceSplit> split : splitsAssignmentSequence) {
            for (Entry<Integer, List<DynamicPscSourceSplit>> assignments :
                    split.assignment().entrySet()) {
                for (DynamicPscSourceSplit assignment : assignments.getValue()) {
                    clusterToTopicPartition
                            .computeIfAbsent(assignment.getPubSubClusterId(), key -> new HashSet<>())
                            .add(assignment.getPscTopicUriPartitionSplit().getTopicUriPartition());
                }
            }
        }

        assertThat(splitsAssigned)
                .hasSize(NUM_SPLITS_PER_CLUSTER * clusterTopicMap.keySet().size());

        // verify correct clusters
        for (String kafkaClusterId : clusterTopicMap.keySet()) {
            assertThat(clusterToTopicPartition)
                    .as("All Kafka clusters must be assigned in the splits.")
                    .containsKey(kafkaClusterId);
        }

        // verify topic partitions
        Set<TopicUriPartition> assignedTopicPartitionSet =
                clusterToTopicPartition.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        Set<TopicUriPartition> expectedTopicPartitions = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : clusterTopicMap.entrySet()) {
            for (String topic : entry.getValue()) {
                for (int i = 0; i < NUM_SPLITS_PER_CLUSTER; i++) {
                    expectedTopicPartitions.add(new TopicUriPartition(kafkaStream.getClusterMetadataMap().get(entry.getKey()).getClusterUriString() + topic, i));
                }
            }
        }
        assertThat(assignedTopicPartitionSet)
                .as("splits must contain all topics and 2 partitions per topic")
                .containsExactlyInAnyOrderElementsOf(expectedTopicPartitions);
    }

    private Map<Integer, Set<DynamicPscSourceSplit>> getReaderAssignments(
            MockSplitEnumeratorContext<DynamicPscSourceSplit> context) {
        Map<Integer, Set<DynamicPscSourceSplit>> readerToSplits = new HashMap<>();
        for (SplitsAssignment<DynamicPscSourceSplit> split :
                context.getSplitsAssignmentSequence()) {
            for (Entry<Integer, List<DynamicPscSourceSplit>> assignments :
                    split.assignment().entrySet()) {
                readerToSplits
                        .computeIfAbsent(assignments.getKey(), key -> new HashSet<>())
                        .addAll(assignments.getValue());
            }
        }
        return readerToSplits;
    }

    private static void runAllOneTimeCallables(MockSplitEnumeratorContext context)
            throws Throwable {
        while (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    private DynamicPscSourceEnumState getCheckpointState(PscStream pscStream)
            throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                new MockPscMetadataService(Collections.singleton(pscStream)),
                                (properties) -> {})) {
            enumerator.start();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(context.getSplitsAssignmentSequence(), pscStream);

            return enumerator.snapshotState(-1);
        }
    }

    private DynamicPscSourceEnumState getCheckpointState() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicPscSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicPscSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC));

            return enumerator.snapshotState(-1);
        }
    }

    private static class TestPscEnumContextProxyFactory
            implements StoppablePscEnumContextProxy.StoppablePscEnumContextProxyFactory {

        @Override
        public StoppablePscEnumContextProxy create(
                SplitEnumeratorContext<DynamicPscSourceSplit> enumContext,
                String clusterId,
                PscMetadataService metadataService,
                Runnable signalNoMoreSplitsCallback) {
            return new TestPscEnumContextProxy(
                    clusterId,
                    metadataService,
                    (MockSplitEnumeratorContext<DynamicPscSourceSplit>) enumContext);
        }
    }

    private static class TestPscEnumContextProxy extends StoppablePscEnumContextProxy {

        private final SplitEnumeratorContext<DynamicPscSourceSplit> enumContext;

        public TestPscEnumContextProxy(
                String clusterId,
                PscMetadataService pscMetadataService,
                MockSplitEnumeratorContext<DynamicPscSourceSplit> enumContext) {
            super(clusterId, pscMetadataService, enumContext, null);
            this.enumContext = enumContext;
        }

        /**
         * Schedule periodic callables under the coordinator executor, so we can use {@link
         * MockSplitEnumeratorContext} to invoke the callable (split assignment) on demand to test
         * the integration of KafkaSourceEnumerator.
         */
        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {
            enumContext.callAsync(
                    wrapCallAsyncCallable(callable),
                    wrapCallAsyncCallableHandler(handler),
                    initialDelay,
                    period);
        }
    }
}
