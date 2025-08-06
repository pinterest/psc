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

package com.pinterest.flink.connector.psc.dynamic.source.reader;

import com.google.common.collect.ImmutableList;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.dynamic.source.MetadataUpdateEvent;
import com.pinterest.flink.connector.psc.dynamic.source.split.DynamicPscSourceSplit;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.streaming.connectors.psc.DynamicPscSourceTestHelperWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.PscTestBaseWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import com.pinterest.psc.serde.IntegerDeserializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link DynamicPscSourceReader}.
 */
public class DynamicPscSourceReaderTest extends SourceReaderTestBase<DynamicPscSourceSplit> {
    private static final String TOPIC = "DynamicPscSourceReaderTest";

    // we are testing two clusters and SourceReaderTestBase expects there to be a total of 10 splits
    private static final int NUM_SPLITS_PER_CLUSTER = 5;

    private static String clusterId0;
    private static String clusterId1;

    @BeforeAll
    static void beforeAll() throws Throwable {
        DynamicPscSourceTestHelperWithKafkaAsPubSub.setup();

        DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(TOPIC, NUM_SPLITS_PER_CLUSTER, 1);
        DynamicPscSourceTestHelperWithKafkaAsPubSub.produceToKafka(
                TOPIC, NUM_SPLITS_PER_CLUSTER, NUM_RECORDS_PER_SPLIT);
        clusterId0 = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPubSubClusterId(0);
        clusterId1 = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPubSubClusterId(1);
    }

    @AfterAll
    static void afterAll() throws Exception {
        DynamicPscSourceTestHelperWithKafkaAsPubSub.tearDown();
    }

    @Test
    void testHandleSourceEventWithRemovedMetadataAtStartup() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (DynamicPscSourceReader<Integer> reader = createReaderWithoutStart(context)) {
            // mock restoring state from Flink runtime
            List<DynamicPscSourceSplit> splits =
                    getSplits(
                            getNumSplits(),
                            NUM_RECORDS_PER_SPLIT,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            reader.addSplits(splits);

            // start reader
            reader.start();
            PscStream pscStream = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC);

            // remove cluster 0
            pscStream.getClusterMetadataMap().remove(clusterId0);
            reader.handleSourceEvents(new MetadataUpdateEvent(Collections.singleton(pscStream)));

            List<DynamicPscSourceSplit> splitsWithoutCluster0 =
                    splits.stream()
                            .filter(split -> !split.getPubSubClusterId().equals(clusterId0))
                            .collect(Collectors.toList());
            assertThat(reader.snapshotState(-1))
                    .as("The splits should not contain any split related to cluster 0")
                    .containsExactlyInAnyOrderElementsOf(splitsWithoutCluster0);
        }
    }

    @Test
    void testNoSubReadersInputStatus() throws Exception {
        try (DynamicPscSourceReader<Integer> reader =
                (DynamicPscSourceReader<Integer>) createReader()) {
            TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
            InputStatus inputStatus = reader.pollNext(readerOutput);
            assertEquals(
                    InputStatus.NOTHING_AVAILABLE,
                    inputStatus,
                    "nothing available since there are no sub readers created, there could be sub readers created in the future");

            // notify that this reader will not be assigned anymore splits
            reader.notifyNoMoreSplits();

            inputStatus = reader.pollNext(readerOutput);
            assertEquals(
                    InputStatus.END_OF_INPUT,
                    inputStatus,
                    "there will not be any more input from this reader since there are no splits");
        }
    }

    @Test
    void testNotifyNoMoreSplits() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (DynamicPscSourceReader<Integer> reader = createReaderWithoutStart(context)) {
            TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
            reader.start();

            // Splits assigned
            List<DynamicPscSourceSplit> splits =
                    getSplits(getNumSplits(), NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED);
            reader.addSplits(splits);

            // Send no more splits
            reader.notifyNoMoreSplits();

            // Send metadata
            MetadataUpdateEvent metadata =
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getMetadataUpdateEvent(TOPIC);
            reader.handleSourceEvents(metadata);

            // Check consistency
            InputStatus status;
            do {
                status = reader.pollNext(readerOutput);
            } while (status != InputStatus.END_OF_INPUT);

            assertThat(readerOutput.getEmittedRecords())
                    .hasSize(getNumSplits() * NUM_RECORDS_PER_SPLIT);
        }
    }

    @Test
    void testAvailabilityFutureUpdates() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (DynamicPscSourceReader<Integer> reader = createReaderWithoutStart(context)) {
            CompletableFuture<Void> futureAtInit = reader.isAvailable();
            assertThat(reader.isActivelyConsumingSplits()).isFalse();
            assertThat(futureAtInit)
                    .as("future is not complete at fresh startup since no readers are created")
                    .isNotDone();
            assertThat(getAvailabilityHelperSize(reader)).isZero();

            reader.start();
            MetadataUpdateEvent metadata =
                    DynamicPscSourceTestHelperWithKafkaAsPubSub.getMetadataUpdateEvent(TOPIC);
            reader.handleSourceEvents(metadata);
            List<DynamicPscSourceSplit> splits =
                    getSplits(
                            getNumSplits(),
                            NUM_RECORDS_PER_SPLIT,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            reader.addSplits(splits);
            CompletableFuture<Void> futureAfterSplitAssignment = reader.isAvailable();

            assertThat(futureAtInit)
                    .as(
                            "New future should have been produced since metadata triggers reader creation")
                    .isNotSameAs(futureAfterSplitAssignment);
            assertThat(getAvailabilityHelperSize(reader)).isEqualTo(2);

            // remove cluster 0
            PscStream kafkaStream = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC);
            kafkaStream.getClusterMetadataMap().remove(clusterId0);
            reader.handleSourceEvents(new MetadataUpdateEvent(Collections.singleton(kafkaStream)));

            CompletableFuture<Void> futureAfterRemovingCluster0 = reader.isAvailable();
            assertThat(futureAfterRemovingCluster0)
                    .as("There should new future since the metadata has changed")
                    .isNotSameAs(futureAfterSplitAssignment);
            assertThat(getAvailabilityHelperSize(reader)).isEqualTo(1);
        }
    }

    private int getAvailabilityHelperSize(DynamicPscSourceReader<?> reader) {
        return ((CompletableFuture<?>[])
                        Whitebox.getInternalState(
                                reader.getAvailabilityHelper(), "futuresToCombine"))
                .length;
    }

    @Test
    void testReaderMetadataChangeWhenOneTopicChanges() throws Exception {
        try (DynamicPscSourceReader<Integer> reader =
                (DynamicPscSourceReader<Integer>) createReader()) {

            // splits with offsets
            DynamicPscSourceSplit cluster0Split =
                    new DynamicPscSourceSplit(
                            DynamicPscSourceTestHelperWithKafkaAsPubSub.getPubSubClusterId(0),
                            new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC, 0), 10));
            DynamicPscSourceSplit cluster1Split =
                    new DynamicPscSourceSplit(
                            DynamicPscSourceTestHelperWithKafkaAsPubSub.getPubSubClusterId(1),
                            new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC, 0), 10));
            reader.addSplits(ImmutableList.of(cluster0Split, cluster1Split));

            // metadata change with a topic changing
            PscStream pscStream = DynamicPscSourceTestHelperWithKafkaAsPubSub.getPscStream(TOPIC);
            Set<String> topicsForCluster1 =
                    pscStream.getClusterMetadataMap().get(clusterId1).getTopics();
            topicsForCluster1.clear();
            topicsForCluster1.add("new topic");
            reader.handleSourceEvents(new MetadataUpdateEvent(Collections.singleton(pscStream)));
            // same split but earlier offset
            DynamicPscSourceSplit newCluster0Split =
                    new DynamicPscSourceSplit(
                            clusterId0,
                            new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC, 0), 10));
            // new split
            DynamicPscSourceSplit newCluster1Split =
                    new DynamicPscSourceSplit(
                            clusterId1,
                            new PscTopicUriPartitionSplit(new TopicUriPartition("new topic", 0), 10));
            reader.addSplits(ImmutableList.of(newCluster0Split, newCluster1Split));

            List<DynamicPscSourceSplit> assignedSplits = reader.snapshotState(-1);

            assertThat(assignedSplits)
                    .as(
                            "The new split for cluster 1 should be assigned and split for cluster 0 should retain offset 10")
                    .containsExactlyInAnyOrder(cluster0Split, newCluster1Split);
        }
    }

    @Override
    protected SourceReader<Integer, DynamicPscSourceSplit> createReader() {
        TestingReaderContext context = new TestingReaderContext();
        return startReader(createReaderWithoutStart(context), context);
    }

    private DynamicPscSourceReader<Integer> createReaderWithoutStart(
            TestingReaderContext context) {
        Properties properties = getRequiredProperties();
        return new DynamicPscSourceReader<>(
                context,
                PscRecordDeserializationSchema.valueOnly(IntegerDeserializer.class),
                properties);
    }

    private SourceReader<Integer, DynamicPscSourceSplit> startReader(
            DynamicPscSourceReader<Integer> reader, TestingReaderContext context) {
        reader.start();
        assertThat(context.getSentEvents())
                .as("Reader sends GetMetadataUpdateEvent at startup")
                .hasSize(1);
        reader.handleSourceEvents(DynamicPscSourceTestHelperWithKafkaAsPubSub.getMetadataUpdateEvent(TOPIC));
        return reader;
    }

    private static Properties getRequiredProperties() {
        Properties properties = new Properties();
        properties.setProperty(
                PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(
                PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER,
                ByteArrayDeserializer.class.getName());
        return properties;
    }

    @Override
    protected List<DynamicPscSourceSplit> getSplits(
            int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        List<DynamicPscSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(getSplit(i, numRecordsPerSplit, boundedness));
        }
        return splits;
    }

    @Override
    protected DynamicPscSourceSplit getSplit(
            int splitId, int numRecords, Boundedness boundedness) {
        long stoppingOffset =
                boundedness == Boundedness.BOUNDED
                        ? NUM_RECORDS_PER_SPLIT
                        : PscTopicUriPartitionSplit.NO_STOPPING_OFFSET;

        String kafkaClusterId;
        int splitIdForCluster = splitId % NUM_SPLITS_PER_CLUSTER;
        int clusterIdx;
        if (splitId < NUM_SPLITS_PER_CLUSTER) {
            clusterIdx = 0;
        } else {
            clusterIdx = 1;
        }

        kafkaClusterId = "pubsub-cluster-" + clusterIdx;

        return new DynamicPscSourceSplit(
                kafkaClusterId,
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(PscTestBaseWithKafkaAsPubSub.clusterUris.get(clusterIdx) + TOPIC, splitIdForCluster), 0L, stoppingOffset));
    }

    @Override
    protected long getNextRecordIndex(DynamicPscSourceSplit split) {
        return split.getPscTopicUriPartitionSplit().getStartingOffset();
    }

    private Map<String, Set<String>> splitsToClusterTopicMap(List<DynamicPscSourceSplit> splits) {
        Map<String, Set<String>> clusterTopicMap = new HashMap<>();

        for (DynamicPscSourceSplit split : splits) {
            Set<String> topics =
                    clusterTopicMap.computeIfAbsent(
                            split.getPubSubClusterId(), (ignore) -> new HashSet<>());
            topics.add(split.getPscTopicUriPartitionSplit().getTopicUri());
        }

        return clusterTopicMap;
    }
}
