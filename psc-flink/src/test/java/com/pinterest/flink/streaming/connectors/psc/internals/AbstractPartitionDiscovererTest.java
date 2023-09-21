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

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.flink.streaming.connectors.psc.testutils.TestTopicUriPartitionDiscoverer;
import com.pinterest.psc.common.TopicUri;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that the partition assignment in the partition discoverer is
 * deterministic and stable, with both fixed and growing partitions.
 */
@RunWith(Parameterized.class)
public class AbstractPartitionDiscovererTest {

    private static final String TOPIC_URI =
            TopicUri.DEFAULT_PROTOCOL + ":" + TopicUri.SEPARATOR + "rn:kafka:env:cloud_region::cluster:myTopic";
    private static final String TOPIC_URI2 =
            TopicUri.DEFAULT_PROTOCOL + ":" + TopicUri.SEPARATOR + "rn:kafka:env:cloud_region::cluster:myTopic2";
    private static final String TEST_TOPIC_PATTERN = "^" + TOPIC_URI + "[0-9]*$";

    private final PscTopicUrisDescriptor topicsDescriptor;

    public AbstractPartitionDiscovererTest(PscTopicUrisDescriptor topicsDescriptor) {
        this.topicsDescriptor = topicsDescriptor;
    }

    @Parameterized.Parameters(name = "PscTopicUrisDescriptor = {0}")
    @SuppressWarnings("unchecked")
    public static Collection<PscTopicUrisDescriptor[]> timeCharacteristic() {
        return Arrays.asList(
                new PscTopicUrisDescriptor[]{new PscTopicUrisDescriptor(Collections.singletonList(TOPIC_URI), null)},
                new PscTopicUrisDescriptor[]{new PscTopicUrisDescriptor(null, Pattern.compile(TEST_TOPIC_PATTERN))});
    }

    @Test
    public void testPartitionsEqualConsumersFixedPartitions() throws Exception {
        List<PscTopicUriPartition> mockGetAllPartitionsForTopicsReturn = Arrays.asList(
                new PscTopicUriPartition(TOPIC_URI, 0),
                new PscTopicUriPartition(TOPIC_URI, 1),
                new PscTopicUriPartition(TOPIC_URI, 2),
                new PscTopicUriPartition(TOPIC_URI, 3));

        int numSubtasks = mockGetAllPartitionsForTopicsReturn.size();

        // get the start index; the assertions below will fail if the assignment logic does not meet correct contracts
        int numConsumers = PscTopicUriPartitionAssigner.assign(mockGetAllPartitionsForTopicsReturn.get(0), numSubtasks);

        for (int subtaskIndex = 0; subtaskIndex < mockGetAllPartitionsForTopicsReturn.size(); subtaskIndex++) {
            TestTopicUriPartitionDiscoverer partitionDiscoverer = new TestTopicUriPartitionDiscoverer(
                    topicsDescriptor,
                    subtaskIndex,
                    mockGetAllPartitionsForTopicsReturn.size(),
                    TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Collections.singletonList(TOPIC_URI)),
                    TestTopicUriPartitionDiscoverer.createMockGetAllPartitionsFromTopicUrisSequenceFromFixedReturn(mockGetAllPartitionsForTopicsReturn));
            partitionDiscoverer.open();

            List<PscTopicUriPartition> initialDiscovery = partitionDiscoverer.discoverPartitions();
            assertEquals(1, initialDiscovery.size());
            assertTrue(contains(mockGetAllPartitionsForTopicsReturn, initialDiscovery.get(0).getPartition()));
            assertEquals(
                    getExpectedSubtaskIndex(initialDiscovery.get(0), numConsumers, numSubtasks),
                    subtaskIndex);

            // subsequent discoveries should not find anything
            List<PscTopicUriPartition> secondDiscovery = partitionDiscoverer.discoverPartitions();
            List<PscTopicUriPartition> thirdDiscovery = partitionDiscoverer.discoverPartitions();
            assertEquals(0, secondDiscovery.size());
            assertEquals(0, thirdDiscovery.size());
        }
    }

    @Test
    public void testMultiplePartitionsPerConsumersFixedPartitions() {
        try {
            final int[] partitionIDs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

            final List<PscTopicUriPartition> mockGetAllPartitionsForTopicsReturn = new ArrayList<>();
            final Set<PscTopicUriPartition> allPartitions = new HashSet<>();

            for (int p : partitionIDs) {
                PscTopicUriPartition part = new PscTopicUriPartition(TOPIC_URI, p);
                mockGetAllPartitionsForTopicsReturn.add(part);
                allPartitions.add(part);
            }

            final int numConsumers = 3;
            final int minPartitionsPerConsumer = mockGetAllPartitionsForTopicsReturn.size() / numConsumers;
            final int maxPartitionsPerConsumer = mockGetAllPartitionsForTopicsReturn.size() / numConsumers + 1;

            // get the start index; the assertions below will fail if the assignment logic does not meet correct contracts
            int startIndex = PscTopicUriPartitionAssigner.assign(mockGetAllPartitionsForTopicsReturn.get(0), numConsumers);

            for (int subtaskIndex = 0; subtaskIndex < numConsumers; subtaskIndex++) {
                TestTopicUriPartitionDiscoverer partitionDiscoverer = new TestTopicUriPartitionDiscoverer(
                        topicsDescriptor,
                        subtaskIndex,
                        numConsumers,
                        TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Collections.singletonList(TOPIC_URI)),
                        TestTopicUriPartitionDiscoverer.createMockGetAllPartitionsFromTopicUrisSequenceFromFixedReturn(mockGetAllPartitionsForTopicsReturn));
                partitionDiscoverer.open();

                List<PscTopicUriPartition> initialDiscovery = partitionDiscoverer.discoverPartitions();
                assertTrue(initialDiscovery.size() >= minPartitionsPerConsumer);
                assertTrue(initialDiscovery.size() <= maxPartitionsPerConsumer);

                for (PscTopicUriPartition p : initialDiscovery) {
                    // check that the element was actually contained
                    assertTrue(allPartitions.remove(p));
                    assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), subtaskIndex);
                }

                // subsequent discoveries should not find anything
                List<PscTopicUriPartition> secondDiscovery = partitionDiscoverer.discoverPartitions();
                List<PscTopicUriPartition> thirdDiscovery = partitionDiscoverer.discoverPartitions();
                assertEquals(0, secondDiscovery.size());
                assertEquals(0, thirdDiscovery.size());
            }

            // all partitions must have been assigned
            assertTrue(allPartitions.isEmpty());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionsFewerThanConsumersFixedPartitions() {
        try {
            List<PscTopicUriPartition> mockGetAllPartitionsForTopicsReturn = Arrays.asList(
                    new PscTopicUriPartition(TOPIC_URI, 0),
                    new PscTopicUriPartition(TOPIC_URI, 1),
                    new PscTopicUriPartition(TOPIC_URI, 2),
                    new PscTopicUriPartition(TOPIC_URI, 3));

            final Set<PscTopicUriPartition> allPartitions = new HashSet<>();
            allPartitions.addAll(mockGetAllPartitionsForTopicsReturn);

            final int numConsumers = 2 * mockGetAllPartitionsForTopicsReturn.size() + 3;

            // get the start index; the assertions below will fail if the assignment logic does not meet correct contracts
            int startIndex = PscTopicUriPartitionAssigner.assign(mockGetAllPartitionsForTopicsReturn.get(0), numConsumers);

            for (int subtaskIndex = 0; subtaskIndex < numConsumers; subtaskIndex++) {
                TestTopicUriPartitionDiscoverer partitionDiscoverer = new TestTopicUriPartitionDiscoverer(
                        topicsDescriptor,
                        subtaskIndex,
                        numConsumers,
                        TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Collections.singletonList(TOPIC_URI)),
                        TestTopicUriPartitionDiscoverer.createMockGetAllPartitionsFromTopicUrisSequenceFromFixedReturn(mockGetAllPartitionsForTopicsReturn));
                partitionDiscoverer.open();

                List<PscTopicUriPartition> initialDiscovery = partitionDiscoverer.discoverPartitions();
                assertTrue(initialDiscovery.size() <= 1);

                for (PscTopicUriPartition p : initialDiscovery) {
                    // check that the element was actually contained
                    assertTrue(allPartitions.remove(p));
                    assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), subtaskIndex);
                }

                // subsequent discoveries should not find anything
                List<PscTopicUriPartition> secondDiscovery = partitionDiscoverer.discoverPartitions();
                List<PscTopicUriPartition> thirdDiscovery = partitionDiscoverer.discoverPartitions();
                assertEquals(0, secondDiscovery.size());
                assertEquals(0, thirdDiscovery.size());
            }

            // all partitions must have been assigned
            assertTrue(allPartitions.isEmpty());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGrowingPartitions() {
        try {
            final int[] newPartitionIDs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            List<PscTopicUriPartition> allPartitions = new ArrayList<>(11);

            for (int p : newPartitionIDs) {
                PscTopicUriPartition part = new PscTopicUriPartition(TOPIC_URI, p);
                allPartitions.add(part);
            }

            // first discovery returns an initial subset of the partitions; second returns all partitions
            List<List<PscTopicUriPartition>> mockGetAllPartitionsForTopicsReturnSequence = Arrays.asList(
                    new ArrayList<>(allPartitions.subList(0, 7)),
                    allPartitions);

            final Set<PscTopicUriPartition> allNewPartitions = new HashSet<>(allPartitions);
            final Set<PscTopicUriPartition> allInitialPartitions = new HashSet<>(mockGetAllPartitionsForTopicsReturnSequence.get(0));

            final int numConsumers = 3;
            final int minInitialPartitionsPerConsumer = mockGetAllPartitionsForTopicsReturnSequence.get(0).size() / numConsumers;
            final int maxInitialPartitionsPerConsumer = mockGetAllPartitionsForTopicsReturnSequence.get(0).size() / numConsumers + 1;
            final int minNewPartitionsPerConsumer = allPartitions.size() / numConsumers;
            final int maxNewPartitionsPerConsumer = allPartitions.size() / numConsumers + 1;

            // get the start index; the assertions below will fail if the assignment logic does not meet correct contracts
            int startIndex = PscTopicUriPartitionAssigner.assign(allPartitions.get(0), numConsumers);

            TestTopicUriPartitionDiscoverer partitionDiscovererSubtask0 = new TestTopicUriPartitionDiscoverer(
                    topicsDescriptor,
                    0,
                    numConsumers,
                    TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Collections.singletonList(TOPIC_URI)),
                    deepClone(mockGetAllPartitionsForTopicsReturnSequence));
            partitionDiscovererSubtask0.open();

            TestTopicUriPartitionDiscoverer partitionDiscovererSubtask1 = new TestTopicUriPartitionDiscoverer(
                    topicsDescriptor,
                    1,
                    numConsumers,
                    TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Collections.singletonList(TOPIC_URI)),
                    deepClone(mockGetAllPartitionsForTopicsReturnSequence));
            partitionDiscovererSubtask1.open();

            TestTopicUriPartitionDiscoverer partitionDiscovererSubtask2 = new TestTopicUriPartitionDiscoverer(
                    topicsDescriptor,
                    2,
                    numConsumers,
                    TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Collections.singletonList(TOPIC_URI)),
                    deepClone(mockGetAllPartitionsForTopicsReturnSequence));
            partitionDiscovererSubtask2.open();

            List<PscTopicUriPartition> initialDiscoverySubtask0 = partitionDiscovererSubtask0.discoverPartitions();
            List<PscTopicUriPartition> initialDiscoverySubtask1 = partitionDiscovererSubtask1.discoverPartitions();
            List<PscTopicUriPartition> initialDiscoverySubtask2 = partitionDiscovererSubtask2.discoverPartitions();

            assertTrue(initialDiscoverySubtask0.size() >= minInitialPartitionsPerConsumer);
            assertTrue(initialDiscoverySubtask0.size() <= maxInitialPartitionsPerConsumer);
            assertTrue(initialDiscoverySubtask1.size() >= minInitialPartitionsPerConsumer);
            assertTrue(initialDiscoverySubtask1.size() <= maxInitialPartitionsPerConsumer);
            assertTrue(initialDiscoverySubtask2.size() >= minInitialPartitionsPerConsumer);
            assertTrue(initialDiscoverySubtask2.size() <= maxInitialPartitionsPerConsumer);

            for (PscTopicUriPartition p : initialDiscoverySubtask0) {
                // check that the element was actually contained
                assertTrue(allInitialPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 0);
            }

            for (PscTopicUriPartition p : initialDiscoverySubtask1) {
                // check that the element was actually contained
                assertTrue(allInitialPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 1);
            }

            for (PscTopicUriPartition p : initialDiscoverySubtask2) {
                // check that the element was actually contained
                assertTrue(allInitialPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 2);
            }

            // all partitions must have been assigned
            assertTrue(allInitialPartitions.isEmpty());

            // now, execute discover again (should find the extra new partitions)
            List<PscTopicUriPartition> secondDiscoverySubtask0 = partitionDiscovererSubtask0.discoverPartitions();
            List<PscTopicUriPartition> secondDiscoverySubtask1 = partitionDiscovererSubtask1.discoverPartitions();
            List<PscTopicUriPartition> secondDiscoverySubtask2 = partitionDiscovererSubtask2.discoverPartitions();

            // new discovered partitions must not have been discovered before
            assertTrue(Collections.disjoint(secondDiscoverySubtask0, initialDiscoverySubtask0));
            assertTrue(Collections.disjoint(secondDiscoverySubtask1, initialDiscoverySubtask1));
            assertTrue(Collections.disjoint(secondDiscoverySubtask2, initialDiscoverySubtask2));

            assertTrue(secondDiscoverySubtask0.size() + initialDiscoverySubtask0.size() >= minNewPartitionsPerConsumer);
            assertTrue(secondDiscoverySubtask0.size() + initialDiscoverySubtask0.size() <= maxNewPartitionsPerConsumer);
            assertTrue(secondDiscoverySubtask1.size() + initialDiscoverySubtask1.size() >= minNewPartitionsPerConsumer);
            assertTrue(secondDiscoverySubtask1.size() + initialDiscoverySubtask1.size() <= maxNewPartitionsPerConsumer);
            assertTrue(secondDiscoverySubtask2.size() + initialDiscoverySubtask2.size() >= minNewPartitionsPerConsumer);
            assertTrue(secondDiscoverySubtask2.size() + initialDiscoverySubtask2.size() <= maxNewPartitionsPerConsumer);

            // check that the two discoveries combined form all partitions

            for (PscTopicUriPartition p : initialDiscoverySubtask0) {
                assertTrue(allNewPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 0);
            }

            for (PscTopicUriPartition p : initialDiscoverySubtask1) {
                assertTrue(allNewPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 1);
            }

            for (PscTopicUriPartition p : initialDiscoverySubtask2) {
                assertTrue(allNewPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 2);
            }

            for (PscTopicUriPartition p : secondDiscoverySubtask0) {
                assertTrue(allNewPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 0);
            }

            for (PscTopicUriPartition p : secondDiscoverySubtask1) {
                assertTrue(allNewPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 1);
            }

            for (PscTopicUriPartition p : secondDiscoverySubtask2) {
                assertTrue(allNewPartitions.remove(p));
                assertEquals(getExpectedSubtaskIndex(p, startIndex, numConsumers), 2);
            }

            // all partitions must have been assigned
            assertTrue(allNewPartitions.isEmpty());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeterministicAssignmentWithDifferentFetchedPartitionOrdering() throws Exception {
        int numSubtasks = 4;

        List<PscTopicUriPartition> mockGetAllPartitionsForTopicsReturn = Arrays.asList(
                new PscTopicUriPartition(TOPIC_URI, 0),
                new PscTopicUriPartition(TOPIC_URI, 1),
                new PscTopicUriPartition(TOPIC_URI, 2),
                new PscTopicUriPartition(TOPIC_URI, 3),
                new PscTopicUriPartition(TOPIC_URI2, 0),
                new PscTopicUriPartition(TOPIC_URI2, 1));

        List<PscTopicUriPartition> mockGetAllPartitionsForTopicsReturnOutOfOrder = Arrays.asList(
                new PscTopicUriPartition(TOPIC_URI, 3),
                new PscTopicUriPartition(TOPIC_URI, 1),
                new PscTopicUriPartition(TOPIC_URI2, 1),
                new PscTopicUriPartition(TOPIC_URI, 0),
                new PscTopicUriPartition(TOPIC_URI2, 0),
                new PscTopicUriPartition(TOPIC_URI, 2));

        for (int subtaskIndex = 0; subtaskIndex < numSubtasks; subtaskIndex++) {
            TestTopicUriPartitionDiscoverer partitionDiscoverer = new TestTopicUriPartitionDiscoverer(
                    topicsDescriptor,
                    subtaskIndex,
                    numSubtasks,
                    TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Arrays.asList(TOPIC_URI, TOPIC_URI2)),
                    TestTopicUriPartitionDiscoverer.createMockGetAllPartitionsFromTopicUrisSequenceFromFixedReturn(mockGetAllPartitionsForTopicsReturn));
            partitionDiscoverer.open();

            TestTopicUriPartitionDiscoverer partitionDiscovererOutOfOrder = new TestTopicUriPartitionDiscoverer(
                    topicsDescriptor,
                    subtaskIndex,
                    numSubtasks,
                    TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Arrays.asList(TOPIC_URI, TOPIC_URI2)),
                    TestTopicUriPartitionDiscoverer.createMockGetAllPartitionsFromTopicUrisSequenceFromFixedReturn(mockGetAllPartitionsForTopicsReturnOutOfOrder));
            partitionDiscovererOutOfOrder.open();

            List<PscTopicUriPartition> discoveredPartitions = partitionDiscoverer.discoverPartitions();
            List<PscTopicUriPartition> discoveredPartitionsOutOfOrder = partitionDiscovererOutOfOrder.discoverPartitions();

            // the subscribed partitions should be identical, regardless of the input partition ordering
            Collections.sort(discoveredPartitions, new PscTopicUriPartition.Comparator());
            Collections.sort(discoveredPartitionsOutOfOrder, new PscTopicUriPartition.Comparator());
            assertEquals(discoveredPartitions, discoveredPartitionsOutOfOrder);
        }
    }

    @Test
    public void testNonContiguousPartitionIdDiscovery() throws Exception {
        List<PscTopicUriPartition> mockGetAllPartitionsForTopicsReturn1 = Arrays.asList(
                new PscTopicUriPartition(TOPIC_URI, 1),
                new PscTopicUriPartition(TOPIC_URI, 4));

        List<PscTopicUriPartition> mockGetAllPartitionsForTopicsReturn2 = Arrays.asList(
                new PscTopicUriPartition(TOPIC_URI, 0),
                new PscTopicUriPartition(TOPIC_URI, 1),
                new PscTopicUriPartition(TOPIC_URI, 2),
                new PscTopicUriPartition(TOPIC_URI, 3),
                new PscTopicUriPartition(TOPIC_URI, 4));

        TestTopicUriPartitionDiscoverer partitionDiscoverer = new TestTopicUriPartitionDiscoverer(
                topicsDescriptor,
                0,
                1,
                TestTopicUriPartitionDiscoverer.createMockGetAllTopicUrisSequenceFromFixedReturn(Collections.singletonList(TOPIC_URI)),
                // first metadata fetch has missing partitions that appears only in the second fetch;
                // need to create new modifiable lists for each fetch, since internally Iterable.remove() is used.
                Arrays.asList(new ArrayList<>(mockGetAllPartitionsForTopicsReturn1), new ArrayList<>(mockGetAllPartitionsForTopicsReturn2)));
        partitionDiscoverer.open();

        List<PscTopicUriPartition> discoveredPartitions1 = partitionDiscoverer.discoverPartitions();
        assertEquals(2, discoveredPartitions1.size());
        assertTrue(discoveredPartitions1.contains(new PscTopicUriPartition(TOPIC_URI, 1)));
        assertTrue(discoveredPartitions1.contains(new PscTopicUriPartition(TOPIC_URI, 4)));

        List<PscTopicUriPartition> discoveredPartitions2 = partitionDiscoverer.discoverPartitions();
        assertEquals(3, discoveredPartitions2.size());
        assertTrue(discoveredPartitions2.contains(new PscTopicUriPartition(TOPIC_URI, 0)));
        assertTrue(discoveredPartitions2.contains(new PscTopicUriPartition(TOPIC_URI, 2)));
        assertTrue(discoveredPartitions2.contains(new PscTopicUriPartition(TOPIC_URI, 3)));
    }

    private boolean contains(List<PscTopicUriPartition> partitions, int partition) {
        for (PscTopicUriPartition ktp : partitions) {
            if (ktp.getPartition() == partition) {
                return true;
            }
        }

        return false;
    }

    private List<List<PscTopicUriPartition>> deepClone(List<List<PscTopicUriPartition>> toClone) {
        List<List<PscTopicUriPartition>> clone = new ArrayList<>(toClone.size());
        for (List<PscTopicUriPartition> partitionsToClone : toClone) {
            List<PscTopicUriPartition> clonePartitions = new ArrayList<>(partitionsToClone.size());
            clonePartitions.addAll(partitionsToClone);

            clone.add(clonePartitions);
        }

        return clone;
    }

    /**
     * Utility method that determines the expected subtask index a partition should be assigned to,
     * depending on the start index and using the partition id as the offset from that start index
     * in clockwise direction.
     *
     * <p>The expectation is based on the distribution contract of
     * {@link PscTopicUriPartitionAssigner#assign(PscTopicUriPartition, int)}.
     */
    private static int getExpectedSubtaskIndex(PscTopicUriPartition partition, int startIndex, int numSubtasks) {
        return (startIndex + partition.getPartition()) % numSubtasks;
    }
}
