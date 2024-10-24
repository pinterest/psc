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

package com.pinterest.flink.connector.psc.source.enumerator;

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.enumerator.subscriber.PscSubscriber;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.connector.psc.testutils.PscSourceTestEnv;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import com.pinterest.psc.metadata.client.PscMetadataClient;
import com.pinterest.psc.serde.StringDeserializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.mock.Whitebox;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link PscSourceEnumerator}. */
public class PscEnumeratorTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String DYNAMIC_TOPIC_NAME = "dynamic_topic";
    private static final String DYNAMIC_TOPIC_URI = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + DYNAMIC_TOPIC_NAME;
    private static final int NUM_PARTITIONS_DYNAMIC_TOPIC = 4;

    private static final String TOPIC1 = "topic";
    private static final String TOPIC_URI1 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + TOPIC1;
    private static final String TOPIC2 = "pattern-topic";
    private static final String TOPIC_URI2 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + TOPIC2;

    private static final int READER0 = 0;
    private static final int READER1 = 1;
    private static final Set<String> PRE_EXISTING_TOPIC_URIS =
            new HashSet<>(Arrays.asList(TOPIC_URI1, TOPIC_URI2));
    private static final Set<String> PRE_EXISTING_TOPICS =
            new HashSet<>(Arrays.asList(TOPIC1, TOPIC2));
    private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
    private static final boolean ENABLE_PERIODIC_PARTITION_DISCOVERY = true;
    private static final boolean DISABLE_PERIODIC_PARTITION_DISCOVERY = false;
    private static final boolean INCLUDE_DYNAMIC_TOPIC = true;
    private static final boolean EXCLUDE_DYNAMIC_TOPIC = false;

    @BeforeClass
    public static void setup() throws Throwable {
        PscSourceTestEnv.setup();
        PscSourceTestEnv.setupTopic(TOPIC_URI1, true, true, PscSourceTestEnv::getRecordsForTopic);
        PscSourceTestEnv.setupTopic(TOPIC_URI2, true, true, PscSourceTestEnv::getRecordsForTopic);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PscSourceTestEnv.tearDown();
    }

    @Test
    public void testStartWithDiscoverPartitionsOnce() throws Exception {
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertTrue(context.getPeriodicCallables().isEmpty());
            assertEquals(
                    "A one time partition discovery callable should have been scheduled",
                    1,
                    context.getOneTimeCallables().size());
        }
    }

    @Test
    public void testStartWithPeriodicPartitionDiscovery() throws Exception {
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertTrue(context.getOneTimeCallables().isEmpty());
            assertEquals(
                    "A periodic partition discovery callable should have been scheduled",
                    1,
                    context.getPeriodicCallables().size());
        }
    }

    @Test
    public void testDiscoverPartitionsTriggersAssignments() throws Throwable {
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();

            // register reader 0.
            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            assertTrue(context.getSplitsAssignmentSequence().isEmpty());

            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);

            // Verify assignments for reader 0.
            verifyLastReadersAssignments(
                    context, Arrays.asList(READER0, READER1), PRE_EXISTING_TOPIC_URIS, 1);
        }
    }

    @Test
    public void testReaderRegistrationTriggersAssignments() throws Throwable {
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            runOneTimePartitionDiscovery(context);
            assertTrue(context.getSplitsAssignmentSequence().isEmpty());

            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER0), PRE_EXISTING_TOPIC_URIS, 1);

            registerReader(context, enumerator, READER1);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER1), PRE_EXISTING_TOPIC_URIS, 2);
        }
    }

    @Test(timeout = 30000L)
    public void testDiscoverPartitionsPeriodically() throws Throwable {
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY,
                                INCLUDE_DYNAMIC_TOPIC);
                AdminClient adminClient = PscSourceTestEnv.getAdminClient()) {

            startEnumeratorAndRegisterReaders(context, enumerator);

            // invoke partition discovery callable again and there should be no new assignments.
            runPeriodicPartitionDiscovery(context);
            assertEquals(
                    "No assignments should be made because there is no partition change",
                    2,
                    context.getSplitsAssignmentSequence().size());

            // create the dynamic topic.
            adminClient
                    .createTopics(
                            Collections.singleton(
                                    new NewTopic(
                                            DYNAMIC_TOPIC_NAME,
                                            NUM_PARTITIONS_DYNAMIC_TOPIC,
                                            (short) 1)))
                    .all()
                    .get();

            // invoke partition discovery callable again.
            while (true) {
                runPeriodicPartitionDiscovery(context);
                if (context.getSplitsAssignmentSequence().size() < 3) {
                    Thread.sleep(10);
                } else {
                    break;
                }
            }
            verifyLastReadersAssignments(
                    context,
                    Arrays.asList(READER0, READER1),
                    Collections.singleton(DYNAMIC_TOPIC_URI),
                    3);
        } finally {
            try (AdminClient adminClient = PscSourceTestEnv.getAdminClient()) {
                adminClient.deleteTopics(Collections.singleton(DYNAMIC_TOPIC_NAME)).all().get();
            } catch (Exception e) {
                // Let it go.
            }
        }
    }

    @Test
    public void testAddSplitsBack() throws Throwable {
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            startEnumeratorAndRegisterReaders(context, enumerator);

            // Simulate a reader failure.
            context.unregisterReader(READER0);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0),
                    READER0);
            assertEquals(
                    "The added back splits should have not been assigned",
                    2,
                    context.getSplitsAssignmentSequence().size());

            // Simulate a reader recovery.
            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER0), PRE_EXISTING_TOPIC_URIS, 3);
        }
    }

    @Test
    public void testWorkWithPreexistingAssignments() throws Throwable {
        Set<TopicUriPartition> preexistingAssignments;
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context1 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(context1, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {
            startEnumeratorAndRegisterReaders(context1, enumerator);
            preexistingAssignments =
                    asEnumState(context1.getSplitsAssignmentSequence().get(0).assignment());
        }

        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context2 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(
                                context2,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY,
                                PRE_EXISTING_TOPICS,
                                preexistingAssignments,
                                new Properties())) {
            enumerator.start();
            runPeriodicPartitionDiscovery(context2);
            registerReader(context2, enumerator, READER0);
            assertTrue(context2.getSplitsAssignmentSequence().isEmpty());

            registerReader(context2, enumerator, READER1);
            verifyLastReadersAssignments(
                    context2, Collections.singleton(READER1), PRE_EXISTING_TOPIC_URIS, 1);
        }
    }

    @Test
    public void testPscClientProperties() throws Exception {
        Properties properties = new Properties();
        String clientIdPrefix = "test-prefix";
        Integer defaultTimeoutMs = 99999;
        properties.setProperty(PscSourceOptions.CLIENT_ID_PREFIX.key(), clientIdPrefix);
        properties.setProperty(
                PscConfiguration.PSC_CONSUMER_POLL_TIMEOUT_MS, String.valueOf(defaultTimeoutMs));
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY,
                                PRE_EXISTING_TOPIC_URIS,
                                Collections.emptySet(),
                                properties)) {
            enumerator.start();

            PscMetadataClient pscMetadataClient =
                    (PscMetadataClient) Whitebox.getInternalState(enumerator, "metadataClient");
            assertNotNull(pscMetadataClient);
            PscConfigurationInternal pscConfigurationInternal = (PscConfigurationInternal) Whitebox.getInternalState(pscMetadataClient, "pscConfigurationInternal");
            assertNotNull(pscConfigurationInternal);
            assertTrue(pscConfigurationInternal.getMetadataClientId().startsWith(clientIdPrefix));
            // TODO: potentially test defaultApiTimeoutMs when it is exposed (it is currently not exposed)
        }
    }

    @Test
    public void testSnapshotState() throws Throwable {
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator = createEnumerator(context, false)) {
            enumerator.start();

            // No reader is registered, so the state should be empty
            final PscSourceEnumState state1 = enumerator.snapshotState(1L);
            assertTrue(state1.assignedPartitions().isEmpty());

            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            runOneTimePartitionDiscovery(context);

            // The state should contain splits assigned to READER0 and READER1
            final PscSourceEnumState state2 = enumerator.snapshotState(1L);
            verifySplitAssignmentWithPartitions(
                    getExpectedAssignments(
                            new HashSet<>(Arrays.asList(READER0, READER1)), PRE_EXISTING_TOPIC_URIS),
                    state2.assignedPartitions());
        }
    }

    @Test
    public void testPartitionChangeChecking() throws Throwable {
        try (MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PscSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {
            enumerator.start();
            runOneTimePartitionDiscovery(context);
            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER0), PRE_EXISTING_TOPIC_URIS, 1);

            // All partitions of TOPIC1 and TOPIC2 should have been discovered now

            // Check partition change using only DYNAMIC_TOPIC_NAME-0
            TopicUriPartition newPartition = new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(DYNAMIC_TOPIC_URI)), 0);
            Set<TopicUriPartition> fetchedPartitions = new HashSet<>();
            fetchedPartitions.add(newPartition);
            final PscSourceEnumerator.PartitionChange partitionChange =
                    enumerator.getPartitionChange(fetchedPartitions);

            // Since enumerator never met DYNAMIC_TOPIC_NAME-0, it should be mark as a new partition
            Set<TopicUriPartition> expectedNewPartitions = Collections.singleton(newPartition);

            // All existing topics are not in the fetchedPartitions, so they should be marked as
            // removed
            Set<TopicUriPartition> expectedRemovedPartitions = new HashSet<>();
            for (int i = 0; i < PscSourceTestEnv.NUM_PARTITIONS; i++) {
                expectedRemovedPartitions.add(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(TOPIC_URI1)), i));
                expectedRemovedPartitions.add(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(TOPIC_URI2)), i));
            }

            assertEquals(expectedNewPartitions, partitionChange.getNewPartitions());
            assertEquals(expectedRemovedPartitions, partitionChange.getRemovedPartitions());
        }
    }

    // -------------- some common startup sequence ---------------

    private void startEnumeratorAndRegisterReaders(
            MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context,
            PscSourceEnumerator enumerator)
            throws Throwable {
        // Start the enumerator and it should schedule a one time task to discover and assign
        // partitions.
        enumerator.start();

        // register reader 0 before the partition discovery.
        registerReader(context, enumerator, READER0);
        assertTrue(context.getSplitsAssignmentSequence().isEmpty());

        // Run the partition discover callable and check the partition assignment.
        runPeriodicPartitionDiscovery(context);
        verifyLastReadersAssignments(
                context, Collections.singleton(READER0), PRE_EXISTING_TOPIC_URIS, 1);

        // Register reader 1 after first partition discovery.
        registerReader(context, enumerator, READER1);
        verifyLastReadersAssignments(
                context, Collections.singleton(READER1), PRE_EXISTING_TOPIC_URIS, 2);
    }

    // ----------------------------------------

    private PscSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PscTopicUriPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery) {
        return createEnumerator(
                enumContext, enablePeriodicPartitionDiscovery, EXCLUDE_DYNAMIC_TOPIC);
    }

    private PscSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PscTopicUriPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            boolean includeDynamicTopic) {
        List<String> topics = new ArrayList<>(PRE_EXISTING_TOPICS);
        if (includeDynamicTopic) {
            topics.add(DYNAMIC_TOPIC_NAME);
        }
        return createEnumerator(
                enumContext,
                enablePeriodicPartitionDiscovery,
                topics,
                Collections.emptySet(),
                new Properties());
    }

    /**
     * Create the enumerator. For the purpose of the tests in this class we don't care about the
     * subscriber and offsets initializer, so just use arbitrary settings.
     */
    private PscSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PscTopicUriPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            Collection<String> topicsToSubscribe,
            Set<TopicUriPartition> assignedPartitions,
            Properties overrideProperties) {
        // Use a TopicPatternSubscriber so that no exception if a subscribed topic hasn't been
        // created yet.
        StringJoiner topicNameJoiner = new StringJoiner("|");
        topicsToSubscribe.forEach(topicNameJoiner::add);
        Pattern topicPattern = Pattern.compile(topicNameJoiner.toString());
        PscSubscriber subscriber = PscSubscriber.getTopicPatternSubscriber(topicPattern);

        OffsetsInitializer startingOffsetsInitializer = OffsetsInitializer.earliest();
        OffsetsInitializer stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();

        Properties props =
                new Properties(PscSourceTestEnv.getConsumerProperties(StringDeserializer.class));
        PscSourceEnumerator.deepCopyProperties(overrideProperties, props);
        String partitionDiscoverInterval = enablePeriodicPartitionDiscovery ? "1" : "-1";
        props.setProperty(
                PscSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                partitionDiscoverInterval);
        props.setProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX);

        return new PscSourceEnumerator(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                enumContext,
                Boundedness.CONTINUOUS_UNBOUNDED,
                assignedPartitions);
    }

    // ---------------------

    private void registerReader(
            MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context,
            PscSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location 0"));
        enumerator.addReader(reader);
    }

    private void verifyLastReadersAssignments(
            MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context,
            Collection<Integer> readers,
            Set<String> topicUris,
            int expectedAssignmentSeqSize) throws TopicUriSyntaxException {
        verifyAssignments(
                getExpectedAssignments(new HashSet<>(readers), topicUris),
                context.getSplitsAssignmentSequence()
                        .get(expectedAssignmentSeqSize - 1)
                        .assignment());
    }

    private void verifyAssignments(
            Map<Integer, Set<TopicUriPartition>> expectedAssignments,
            Map<Integer, List<PscTopicUriPartitionSplit>> actualAssignments) {
        actualAssignments.forEach(
                (reader, splits) -> {
                    Set<TopicUriPartition> expectedAssignmentsForReader =
                            expectedAssignments.get(reader);
                    assertNotNull(expectedAssignmentsForReader);
                    assertEquals(expectedAssignmentsForReader.size(), splits.size());
                    for (PscTopicUriPartitionSplit split : splits) {
                        assertTrue(
                                expectedAssignmentsForReader.contains(split.getTopicUriPartition()));
                    }
                });
    }

    private Map<Integer, Set<TopicUriPartition>> getExpectedAssignments(
            Set<Integer> readers, Set<String> topicUriStrings) throws TopicUriSyntaxException {
        Map<Integer, Set<TopicUriPartition>> expectedAssignments = new HashMap<>();
        Set<TopicUriPartition> allPartitions = new HashSet<>();

        if (topicUriStrings.contains(DYNAMIC_TOPIC_URI)) {
            for (int i = 0; i < NUM_PARTITIONS_DYNAMIC_TOPIC; i++) {
                allPartitions.add(new TopicUriPartition(KafkaTopicUri.validate(BaseTopicUri.validate(DYNAMIC_TOPIC_URI)), i));
            }
        }

        for (TopicUriPartition tp : PscSourceTestEnv.getPartitionsForTopics(PRE_EXISTING_TOPIC_URIS)) {
            if (topicUriStrings.contains(tp.getTopicUriAsString())) {
                allPartitions.add(tp);
            }
        }

        for (TopicUriPartition tp : allPartitions) {
            int ownerReader = PscSourceEnumerator.getSplitOwner(tp, NUM_SUBTASKS);
            if (readers.contains(ownerReader)) {
                expectedAssignments.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(tp);
            }
        }
        return expectedAssignments;
    }

    private void verifySplitAssignmentWithPartitions(
            Map<Integer, Set<TopicUriPartition>> expectedAssignment,
            Set<TopicUriPartition> actualTopicUriPartitions) {
        final Set<TopicUriPartition> allTopicUriPartitionsFromAssignment = new HashSet<>();
        expectedAssignment.forEach(
                (reader, topicPartitions) ->
                        allTopicUriPartitionsFromAssignment.addAll(topicPartitions));
        assertEquals(allTopicUriPartitionsFromAssignment, actualTopicUriPartitions);
    }

    private Set<TopicUriPartition> asEnumState(Map<Integer, List<PscTopicUriPartitionSplit>> assignments) {
        Set<TopicUriPartition> enumState = new HashSet<>();
        assignments.forEach(
                (reader, assignment) ->
                        assignment.forEach(split -> enumState.add(split.getTopicUriPartition())));
        return enumState;
    }

    private void runOneTimePartitionDiscovery(
            MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context) throws Throwable {
        // Fetch potential topic descriptions
        context.runNextOneTimeCallable();
        // Initialize offsets for discovered partitions
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    private void runPeriodicPartitionDiscovery(
            MockSplitEnumeratorContext<PscTopicUriPartitionSplit> context) throws Throwable {
        // Fetch potential topic descriptions
        context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
        // Initialize offsets for discovered partitions
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    // -------------- private class ----------------

    private static class BlockingClosingContext
            extends MockSplitEnumeratorContext<PscTopicUriPartitionSplit> {

        public BlockingClosingContext(int parallelism) {
            super(parallelism);
        }

        @Override
        public void close() {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                // let it go.
            }
        }
    }
}
