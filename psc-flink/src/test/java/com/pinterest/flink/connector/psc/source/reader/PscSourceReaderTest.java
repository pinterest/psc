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

package com.pinterest.flink.connector.psc.source.reader;

import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.source.PscSource;
import com.pinterest.flink.connector.psc.source.PscSourceBuilder;
import com.pinterest.flink.connector.psc.source.PscSourceOptions;
import com.pinterest.flink.connector.psc.source.PscSourceTestUtils;
import com.pinterest.flink.connector.psc.source.enumerator.initializer.OffsetsInitializer;
import com.pinterest.flink.connector.psc.source.reader.deserializer.PscRecordDeserializationSchema;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.connector.psc.testutils.PscSourceTestEnv;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.IntegerDeserializer;
import com.pinterest.psc.serde.IntegerSerializer;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.COMMITS_SUCCEEDED_METRIC_COUNTER;
import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.COMMITTED_OFFSET_METRIC_GAUGE;
import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE;
import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.INITIAL_OFFSET;
import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.PARTITION_GROUP;
import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.PSC_CONSUMER_METRIC_GROUP;
import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.PSC_SOURCE_READER_METRIC_GROUP;
import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.TOPIC_URI_GROUP;
import static com.pinterest.flink.connector.psc.testutils.PscSourceTestEnv.NUM_PARTITIONS;
import static com.pinterest.flink.connector.psc.testutils.PscTestUtils.putDiscoveryProperties;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PscSourceReader}. */
public class PscSourceReaderTest extends SourceReaderTestBase<PscTopicUriPartitionSplit> {
    private static final String TOPIC = "PscSourceReaderTest";
    private static final String TOPIC_URI_STR = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + TOPIC;

    @BeforeAll
    public static void setup() throws Throwable {
        PscSourceTestEnv.setup();
        try (AdminClient adminClient = PscSourceTestEnv.getAdminClient()) {
            adminClient
                    .createTopics(
                            Collections.singleton(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1)))
                    .all()
                    .get();
            // Use the admin client to trigger the creation of internal __consumer_offsets topic.
            // This makes sure that we won't see unavailable coordinator in the tests.
            waitUtil(
                    () -> {
                        try {
                            adminClient
                                    .listConsumerGroupOffsets("AnyGroup")
                                    .partitionsToOffsetAndMetadata()
                                    .get();
                        } catch (Exception e) {
                            return false;
                        }
                        return true;
                    },
                    Duration.ofSeconds(60),
                    "Waiting for offsets topic creation failed.");
        }
        PscSourceTestEnv.produceMessages(
                getRecords(), StringSerializer.class, IntegerSerializer.class);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        PscSourceTestEnv.tearDown();
    }

    protected int getNumSplits() {
        return NUM_PARTITIONS;
    }

    // -----------------------------------------

    @Test
    void testCommitOffsetsWithoutAliveFetchers() throws Exception {
        final String groupId = "testCommitOffsetsWithoutAliveFetchers";
        try (PscSourceReader<Integer> reader =
                (PscSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            PscTopicUriPartitionSplit split =
                    new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC_URI_STR, 0), 0, NUM_RECORDS_PER_SPLIT);
            reader.addSplits(Collections.singletonList(split));
            reader.notifyNoMoreSplits();
            ReaderOutput<Integer> output = new TestingReaderOutput<>();
            InputStatus status;
            do {
                status = reader.pollNext(output);
            } while (status != InputStatus.NOTHING_AVAILABLE);
            pollUntil(
                    reader,
                    output,
                    () -> reader.getNumAliveFetchers() == 0,
                    "The split fetcher did not exit before timeout.");
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
            // Due to a bug in KafkaConsumer, when the consumer closes, the offset commit callback
            // won't be fired, so the offsetsToCommit map won't be cleaned. To make the test
            // stable, we add a split whose starting offset is the log end offset, so the
            // split fetcher won't become idle and exit after commitOffsetAsync is invoked from
            // notifyCheckpointComplete().
            reader.addSplits(
                    Collections.singletonList(
                            new PscTopicUriPartitionSplit(
                                    new TopicUriPartition(TOPIC_URI_STR, 0), NUM_RECORDS_PER_SPLIT)));
            pollUntil(
                    reader,
                    output,
                    () -> reader.getOffsetsToCommit().isEmpty(),
                    "The offset commit did not finish before timeout.");
        }
        // Verify the committed offsets.
        try (AdminClient adminClient = PscSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertThat(committedOffsets).hasSize(1);
            assertThat(committedOffsets.values())
                    .extracting(OffsetAndMetadata::offset)
                    .allMatch(offset -> offset == NUM_RECORDS_PER_SPLIT);
        }
    }

    @Test
    void testCommitEmptyOffsets() throws Exception {
        final String groupId = "testCommitEmptyOffsets";
        try (PscSourceReader<Integer> reader =
                (PscSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
        }
        // Verify the committed offsets.
        try (AdminClient adminClient = PscSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertThat(committedOffsets).isEmpty();
        }
    }

    @Test
    void testOffsetCommitOnCheckpointComplete() throws Exception {
        final String groupId = "testOffsetCommitOnCheckpointComplete";
        try (PscSourceReader<Integer> reader =
                (PscSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            reader.addSplits(
                    getSplits(numSplits, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED));
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            long checkpointId = 0;
            do {
                checkpointId++;
                reader.pollNext(output);
                // Create a checkpoint for each message consumption, but not complete them.
                reader.snapshotState(checkpointId);
            } while (output.count() < totalNumRecords);

            // The completion of the last checkpoint should subsume all the previous checkpoitns.
            assertThat(reader.getOffsetsToCommit()).hasSize((int) checkpointId);

            long lastCheckpointId = checkpointId;
            waitUtil(
                    () -> {
                        try {
                            reader.notifyCheckpointComplete(lastCheckpointId);
                        } catch (Exception exception) {
                            throw new RuntimeException(
                                    "Caught unexpected exception when polling from the reader",
                                    exception);
                        }
                        return reader.getOffsetsToCommit().isEmpty();
                    },
                    Duration.ofSeconds(60),
                    Duration.ofSeconds(1),
                    "The offset commit did not finish before timeout.");
        }

        // Verify the committed offsets.
        try (AdminClient adminClient = PscSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertThat(committedOffsets).hasSize(numSplits);
            assertThat(committedOffsets.values())
                    .extracting(OffsetAndMetadata::offset)
                    .allMatch(offset -> offset == NUM_RECORDS_PER_SPLIT);
        }
    }

    @Test
    void testNotCommitOffsetsForUninitializedSplits() throws Exception {
        final long checkpointId = 1234L;
        try (PscSourceReader<Integer> reader = (PscSourceReader<Integer>) createReader()) {
            PscTopicUriPartitionSplit split =
                    new PscTopicUriPartitionSplit(
                            new TopicUriPartition(TOPIC_URI_STR, 0), PscTopicUriPartitionSplit.EARLIEST_OFFSET);
            reader.addSplits(Collections.singletonList(split));
            reader.snapshotState(checkpointId);
            assertThat(reader.getOffsetsToCommit()).hasSize(1);
            assertThat(reader.getOffsetsToCommit().get(checkpointId)).isEmpty();
        }
    }

    @Test
    void testDisableOffsetCommit() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(PscSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false");
        try (PscSourceReader<Integer> reader =
                (PscSourceReader<Integer>)
                        createReader(
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new TestingReaderContext(),
                                (ignore) -> {},
                                properties)) {
            reader.addSplits(
                    getSplits(numSplits, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED));
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            long checkpointId = 0;
            do {
                checkpointId++;
                reader.pollNext(output);
                // Create a checkpoint for each message consumption, but not complete them.
                reader.snapshotState(checkpointId);
                // Offsets to commit should be always empty because offset commit is disabled
                assertThat(reader.getOffsetsToCommit()).isEmpty();
            } while (output.count() < totalNumRecords);
        }
    }

    @Test
    void testPscSourceMetrics() throws Exception {
        final MetricListener metricListener = new MetricListener();
        final String groupId = "testPscSourceMetrics";
        final TopicUriPartition tp0 = new TopicUriPartition(TOPIC_URI_STR, 0);
        final TopicUriPartition tp1 = new TopicUriPartition(TOPIC_URI_STR, 1);

        try (PscSourceReader<Integer> reader =
                (PscSourceReader<Integer>)
                        createReader(
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                groupId,
                                metricListener.getMetricGroup())) {

            PscTopicUriPartitionSplit split0 =
                    new PscTopicUriPartitionSplit(tp0, PscTopicUriPartitionSplit.EARLIEST_OFFSET);
            PscTopicUriPartitionSplit split1 =
                    new PscTopicUriPartitionSplit(tp1, PscTopicUriPartitionSplit.EARLIEST_OFFSET);
            reader.addSplits(Arrays.asList(split0, split1));

            TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
            pollUntil(
                    reader,
                    output,
                    () -> output.getEmittedRecords().size() == NUM_RECORDS_PER_SPLIT * 2,
                    String.format(
                            "Failed to poll %d records until timeout", NUM_RECORDS_PER_SPLIT * 2));
            Thread.sleep(100); // Wait for the metric to be updated
            // Metric "records-consumed-total" of KafkaConsumer should be NUM_RECORDS_PER_SPLIT
            assertThat(getPscConsumerMetric("records-consumed-total", metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT * 2);

            // Current consuming offset should be NUM_RECORD_PER_SPLIT - 1
            assertThat(getCurrentOffsetMetric(tp0, metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT - 1);
            assertThat(getCurrentOffsetMetric(tp1, metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT - 1);

            // No offset is committed till now
            assertThat(getCommittedOffsetMetric(tp0, metricListener)).isEqualTo(INITIAL_OFFSET);
            assertThat(getCommittedOffsetMetric(tp1, metricListener)).isEqualTo(INITIAL_OFFSET);

            // Trigger offset commit
            final long checkpointId = 15213L;
            reader.snapshotState(checkpointId);
            waitUtil(
                    () -> {
                        try {
                            reader.notifyCheckpointComplete(checkpointId);
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to notify checkpoint complete to reader", e);
                        }
                        return reader.getOffsetsToCommit().isEmpty();
                    },
                    Duration.ofSeconds(60),
                    Duration.ofSeconds(1),
                    String.format(
                            "Offsets are not committed successfully. Dangling offsets: %s",
                            reader.getOffsetsToCommit()));

            // Metric "commit-total" of KafkaConsumer should be greater than 0
            // It's hard to know the exactly number of commit because of the retry
            MatcherAssert.assertThat(
                    getPscConsumerMetric("commit-total", metricListener),
                    Matchers.greaterThan(0L));

            // Committed offset should be NUM_RECORD_PER_SPLIT
            assertThat(getCommittedOffsetMetric(tp0, metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT);
            assertThat(getCommittedOffsetMetric(tp1, metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT);

            // Number of successful commits should be greater than 0
            final Optional<Counter> commitsSucceeded =
                    metricListener.getCounter(
                            PSC_SOURCE_READER_METRIC_GROUP, COMMITS_SUCCEEDED_METRIC_COUNTER);
            assertThat(commitsSucceeded).isPresent();
            MatcherAssert.assertThat(commitsSucceeded.get().getCount(), Matchers.greaterThan(0L));
        }
    }

    @Test
    void testAssigningEmptySplits() throws Exception {
        // Normal split with NUM_RECORDS_PER_SPLIT records
        final PscTopicUriPartitionSplit normalSplit =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC_URI_STR, 0), 0, PscTopicUriPartitionSplit.LATEST_OFFSET);
        // Empty split with no record
        final PscTopicUriPartitionSplit emptySplit =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC_URI_STR, 1), NUM_RECORDS_PER_SPLIT, NUM_RECORDS_PER_SPLIT);
        // Split finished hook for listening finished splits
        final Set<String> finishedSplits = new HashSet<>();
        final Consumer<Collection<String>> splitFinishedHook = finishedSplits::addAll;

        try (final PscSourceReader<Integer> reader =
                (PscSourceReader<Integer>)
                        createReader(
                                Boundedness.BOUNDED,
                                "KafkaSourceReaderTestGroup",
                                new TestingReaderContext(),
                                splitFinishedHook)) {
            reader.addSplits(Arrays.asList(normalSplit, emptySplit));
            pollUntil(
                    reader,
                    new TestingReaderOutput<>(),
                    () -> reader.getNumAliveFetchers() == 0,
                    "The split fetcher did not exit before timeout.");
            MatcherAssert.assertThat(
                    finishedSplits,
                    Matchers.containsInAnyOrder(
                            PscTopicUriPartitionSplit.toSplitId(normalSplit.getTopicUriPartition()),
                            PscTopicUriPartitionSplit.toSplitId(emptySplit.getTopicUriPartition())));
        }
    }

    @Test
    void testAssigningEmptySplitOnly() throws Exception {
        // Empty split with no record
        PscTopicUriPartitionSplit emptySplit0 =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC_URI_STR, 0), NUM_RECORDS_PER_SPLIT, NUM_RECORDS_PER_SPLIT);
        PscTopicUriPartitionSplit emptySplit1 =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC_URI_STR, 1), NUM_RECORDS_PER_SPLIT, NUM_RECORDS_PER_SPLIT);
        // Split finished hook for listening finished splits
        final Set<String> finishedSplits = new HashSet<>();
        final Consumer<Collection<String>> splitFinishedHook = finishedSplits::addAll;

        try (final PscSourceReader<Integer> reader =
                (PscSourceReader<Integer>)
                        createReader(
                                Boundedness.BOUNDED,
                                "PscSourceReaderTestGroup",
                                new TestingReaderContext(),
                                splitFinishedHook)) {
            reader.addSplits(Arrays.asList(emptySplit0, emptySplit1));
            pollUntil(
                    reader,
                    new TestingReaderOutput<>(),
                    () -> reader.getNumAliveFetchers() == 0,
                    "The split fetcher did not exit before timeout.");
            assertThat(reader.getNumAliveFetchers()).isEqualTo(0);

            // upstream asserts containsExactly (in order) but that is not necessary given that finishedSplits is a Set
            assertThat(finishedSplits)
                    .contains(emptySplit0.splitId(), emptySplit1.splitId());
        }
    }

    // ------------------------------------------

    @Override
    protected SourceReader<Integer, PscTopicUriPartitionSplit> createReader() throws Exception {
        return createReader(Boundedness.BOUNDED, "KafkaSourceReaderTestGroup");
    }

    @Override
    protected List<PscTopicUriPartitionSplit> getSplits(
            int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        List<PscTopicUriPartitionSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(getSplit(i, numRecordsPerSplit, boundedness));
        }
        return splits;
    }

    @Override
    protected PscTopicUriPartitionSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
        long stoppingOffset =
                boundedness == Boundedness.BOUNDED
                        ? NUM_RECORDS_PER_SPLIT
                        : PscTopicUriPartitionSplit.NO_STOPPING_OFFSET;
        return new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC_URI_STR, splitId), 0L, stoppingOffset);
    }

    @Override
    protected long getNextRecordIndex(PscTopicUriPartitionSplit split) {
        return split.getStartingOffset();
    }

    // ---------------------

    private SourceReader<Integer, PscTopicUriPartitionSplit> createReader(
            Boundedness boundedness, String groupId) throws Exception {
        return createReader(boundedness, groupId, new TestingReaderContext(), (ignore) -> {});
    }

    private SourceReader<Integer, PscTopicUriPartitionSplit> createReader(
            Boundedness boundedness, String groupId, MetricGroup metricGroup) throws Exception {
        return createReader(
                boundedness,
                groupId,
                new TestingReaderContext(
                        new Configuration(), InternalSourceReaderMetricGroup.mock(metricGroup)),
                (ignore) -> {});
    }

    private SourceReader<Integer, PscTopicUriPartitionSplit> createReader(
            Boundedness boundedness,
            String groupId,
            SourceReaderContext context,
            Consumer<Collection<String>> splitFinishedHook)
            throws Exception {
        Properties properties = new Properties();
        properties.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, groupId);
        return createReader(boundedness, context, splitFinishedHook, properties);
    }

    private SourceReader<Integer, PscTopicUriPartitionSplit> createReader(
            Boundedness boundedness,
            SourceReaderContext context,
            Consumer<Collection<String>> splitFinishedHook,
            Properties props)
            throws Exception {
        props.setProperty(PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX);
        props.setProperty(PscConfiguration.PSC_METRICS_FREQUENCY_MS, "100");
        if (!props.containsKey(PscConfiguration.PSC_CONSUMER_GROUP_ID)) {
            props.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group-id");
        }
        putDiscoveryProperties(props, PscSourceTestEnv.getBrokerConnectionStrings(), PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX);
        PscSourceBuilder<Integer> builder =
                PscSource.<Integer>builder()
                        .setClientIdPrefix("PscSourceReaderTest")
                        .setDeserializer(
                                PscRecordDeserializationSchema.valueOnly(
                                        IntegerDeserializer.class))
                        .setPartitions(Collections.singleton(new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + "AnyTopic", 0)))
//                        .setProperty(
//                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
//                                KafkaSourceTestEnv.brokerConnectionStrings)
                        .setProperty(PscConfiguration.PSC_CONSUMER_COMMIT_AUTO_ENABLED, "false")
                        .setProperties(props);
        if (boundedness == Boundedness.BOUNDED) {
            builder.setBounded(OffsetsInitializer.latest());
        }

        return PscSourceTestUtils.createReaderWithFinishedSplitHook(
                builder.build(), context, splitFinishedHook);
    }

    private void pollUntil(
            PscSourceReader<Integer> reader,
            ReaderOutput<Integer> output,
            Supplier<Boolean> condition,
            String errorMessage)
            throws Exception {
        waitUtil(
                () -> {
                    try {
                        reader.pollNext(output);
                    } catch (Exception exception) {
                        throw new RuntimeException(
                                "Caught unexpected exception when polling from the reader",
                                exception);
                    }
                    return condition.get();
                },
                Duration.ofSeconds(60),
                errorMessage);
    }

    private long getPscConsumerMetric(String name, MetricListener listener) {
        final Optional<Gauge<Object>> kafkaConsumerGauge =
                listener.getGauge(
                        PSC_SOURCE_READER_METRIC_GROUP, PSC_CONSUMER_METRIC_GROUP, name);
        assertThat(kafkaConsumerGauge).isPresent();
        return ((Double) kafkaConsumerGauge.get().getValue()).longValue();
    }

    private long getCurrentOffsetMetric(TopicUriPartition tp, MetricListener listener) {
        final Optional<Gauge<Object>> currentOffsetGauge =
                listener.getGauge(
                        PSC_SOURCE_READER_METRIC_GROUP,
                        TOPIC_URI_GROUP,
                        tp.getTopicUriAsString(),
                        PARTITION_GROUP,
                        String.valueOf(tp.getPartition()),
                        CURRENT_OFFSET_METRIC_GAUGE);
        assertThat(currentOffsetGauge).isPresent();
        return (long) currentOffsetGauge.get().getValue();
    }

    private long getCommittedOffsetMetric(TopicUriPartition tp, MetricListener listener) {
        final Optional<Gauge<Object>> committedOffsetGauge =
                listener.getGauge(
                        PSC_SOURCE_READER_METRIC_GROUP,
                        TOPIC_URI_GROUP,
                        tp.getTopicUriAsString(),
                        PARTITION_GROUP,
                        String.valueOf(tp.getPartition()),
                        COMMITTED_OFFSET_METRIC_GAUGE);
        assertThat(committedOffsetGauge).isPresent();
        return (long) committedOffsetGauge.get().getValue();
    }

    // ---------------------

    private static List<PscProducerMessage<String, Integer>> getRecords() {
        List<PscProducerMessage<String, Integer>> records = new ArrayList<>();
        for (int part = 0; part < NUM_PARTITIONS; part++) {
            for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
                records.add(
                        new PscProducerMessage<>(
                                TOPIC_URI_STR, part, TOPIC + "-" + part, part * NUM_RECORDS_PER_SPLIT + i));
            }
        }
        return records;
    }
}
