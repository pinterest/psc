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

import com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.connector.psc.testutils.PscSourceTestEnv;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.serde.ByteArrayDeserializer;
import com.pinterest.psc.serde.IntegerDeserializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.pinterest.flink.connector.psc.testutils.PscSourceTestEnv.NUM_RECORDS_PER_PARTITION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PscTopicUriPartitionSplitReader}. */
public class PscTopicUriPartitionSplitReaderTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC_URI1 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + TOPIC1;
    private static final String TOPIC2 = "topic2";
    private static final String TOPIC_URI2 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + TOPIC2;
    private static final String TOPIC3 = "topic3";
    private static final String TOPIC_URI3 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + TOPIC3;

    private static Map<Integer, Map<String, PscTopicUriPartitionSplit>> splitsByOwners;
    private static Map<TopicUriPartition, Long> earliestOffsets;

    private final IntegerDeserializer deserializer = new IntegerDeserializer();

    @BeforeAll
    public static void setup() throws Throwable {
        PscSourceTestEnv.setup();
        PscSourceTestEnv.setupTopic(TOPIC_URI1, true, true, PscSourceTestEnv::getRecordsForTopic);
        PscSourceTestEnv.setupTopic(TOPIC_URI2, true, true, PscSourceTestEnv::getRecordsForTopic);
        PscSourceTestEnv.createTestTopic(TOPIC_URI3);
        splitsByOwners =
                PscSourceTestEnv.getSplitsByOwners(Arrays.asList(TOPIC_URI1, TOPIC_URI2), NUM_SUBTASKS);
        earliestOffsets =
                PscSourceTestEnv.getEarliestOffsets(
                        PscSourceTestEnv.getPartitionsForTopics(Arrays.asList(TOPIC_URI1, TOPIC_URI2)));
    }

    @AfterAll
    public static void tearDown() throws Exception {
        PscSourceTestEnv.tearDown();
    }

    @Test
    public void testHandleSplitChangesAndFetch() throws Exception {
        PscTopicUriPartitionSplitReader reader = createReader();
        assignSplitsAndFetchUntilFinish(reader, 0);
        assignSplitsAndFetchUntilFinish(reader, 1);
    }

    @Test
    public void testWakeUp() throws Exception {
        PscTopicUriPartitionSplitReader reader = createReader();
        TopicUriPartition nonExistingTopicPartition = new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + "NotExist", 0);
        assignSplits(
                reader,
                Collections.singletonMap(
                        PscTopicUriPartitionSplit.toSplitId(nonExistingTopicPartition),
                        new PscTopicUriPartitionSplit(nonExistingTopicPartition, 0)));
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t =
                new Thread(
                        () -> {
                            try {
                                reader.fetch();
                            } catch (Throwable e) {
                                error.set(e);
                            }
                        },
                        "testWakeUp-thread");
        t.start();
        long deadline = System.currentTimeMillis() + 5000L;
        while (t.isAlive() && System.currentTimeMillis() < deadline) {
            reader.wakeUp();
            Thread.sleep(10);
        }
        assertThat(error.get()).isNull();
    }

    @Test
    public void testWakeupThenAssign() throws IOException, ConfigurationException, ClientException {
        PscTopicUriPartitionSplitReader reader = createReader();
        // Assign splits with records
        assignSplits(reader, splitsByOwners.get(0));
        // Run a fetch operation, and it should not block
        reader.fetch();
        // Wake the reader up then assign a new split. This assignment should not throw
        // WakeupException.
        reader.wakeUp();
        TopicUriPartition tp = new TopicUriPartition(TOPIC_URI1, 0);
        assignSplits(
                reader,
                Collections.singletonMap(
                        PscTopicUriPartitionSplit.toSplitId(tp),
                        new PscTopicUriPartitionSplit(tp, PscTopicUriPartitionSplit.EARLIEST_OFFSET)));
    }

    @Test
    public void testNumBytesInCounter() throws Exception {
        final OperatorMetricGroup operatorMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        final Counter numBytesInCounter =
                operatorMetricGroup.getIOMetricGroup().getNumBytesInCounter();
        Properties props = new Properties();
        props.setProperty(PscConfiguration.PSC_METRICS_FREQUENCY_MS, "100");
        PscTopicUriPartitionSplitReader reader =
                createReader(
                        props,
                        InternalSourceReaderMetricGroup.wrap(operatorMetricGroup));
        // Add a split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC_URI1, 0), 0L))));
        reader.fetch();
        Thread.sleep(100); // wait for metrics to be updated
        reader.fetch(); // second fetch should be no-op but it is needed to ensure numBytesIn is properly updated
        final long latestNumBytesIn = numBytesInCounter.getCount();
        // Since it's hard to know the exact number of bytes consumed, we just check if it is
        // greater than 0
        assertThat(latestNumBytesIn).isGreaterThan(0L);
        // Add another split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC_URI2, 0), 0L))));
        reader.fetch();
        Thread.sleep(100); // wait for metrics to be updated
        reader.fetch(); // second fetch should be no-op but it is needed to ensure numBytesIn is properly updated
        // We just check if numBytesIn is increasing
        assertThat(numBytesInCounter.getCount()).isGreaterThan(latestNumBytesIn);
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"_underscore.period-minus"})
    @Disabled("This test is flaky due to records-lag metric not present, instead we use records-lag-max in a 30 second window." +
            " Concurrency of validations and metric updates in native KafkaConsumer causes flakiness.")
    public void testPendingRecordsGauge(String topicSuffix) throws Throwable {
        final String topic1UriStr = TOPIC_URI1 + topicSuffix;
        final String topic2UriStr = TOPIC_URI2 + topicSuffix;
        if (!topicSuffix.isEmpty()) {
            PscSourceTestEnv.setupTopic(
                    topic1UriStr, true, true, PscSourceTestEnv::getRecordsForTopic);
            PscSourceTestEnv.setupTopic(
                    topic2UriStr, true, true, PscSourceTestEnv::getRecordsForTopic);
        }
        MetricListener metricListener = new MetricListener();
        final Properties props = new Properties();
        props.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, "1");
        props.setProperty(PscConfiguration.PSC_METRICS_FREQUENCY_MS, "100");
        props.setProperty("psc.consumer." + ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "500");
        PscTopicUriPartitionSplitReader reader =
                createReader(
                        props,
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        // Add a split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(new TopicUriPartition(topic1UriStr, 0), 0L))));
        // pendingRecords should have not been registered because of lazily registration
        assertThat(metricListener.getGauge(MetricNames.PENDING_RECORDS)).isNotPresent();
        // Trigger first fetch
        reader.fetch();
        final Optional<Gauge<Long>> pendingRecords =
                metricListener.getGauge(MetricNames.PENDING_RECORDS);
        assertThat(pendingRecords).isPresent();
        // Validate pendingRecords
        assertThat(pendingRecords).isNotNull();
        assertThat((long) pendingRecords.get().getValue()).isEqualTo(NUM_RECORDS_PER_PARTITION - 1);
        for (int i = 1; i < NUM_RECORDS_PER_PARTITION; i++) {
            reader.fetch();
            assertThat((long) pendingRecords.get().getValue())
                    .isEqualTo(NUM_RECORDS_PER_PARTITION - i - 1);
        }
        // Add another split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(new TopicUriPartition(topic2UriStr, 0), 0L))));
        // Validate pendingRecords
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            reader.fetch();
            assertThat((long) pendingRecords.get().getValue())
                    .isEqualTo(NUM_RECORDS_PER_PARTITION - i - 1);
        }
    }

    @Test
    public void testAssignEmptySplit() throws Exception {
        PscTopicUriPartitionSplitReader reader = createReader();
        final PscTopicUriPartitionSplit normalSplit =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC_URI1, 0),
                        PscTopicUriPartitionSplit.EARLIEST_OFFSET,
                        PscTopicUriPartitionSplit.NO_STOPPING_OFFSET);
        final PscTopicUriPartitionSplit emptySplit =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC_URI2, 0),
                        PscSourceTestEnv.NUM_RECORDS_PER_PARTITION,
                        PscSourceTestEnv.NUM_RECORDS_PER_PARTITION);
        final PscTopicUriPartitionSplit emptySplitWithZeroStoppingOffset =
                new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC_URI3, 0), 0, 0);

        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Arrays.asList(normalSplit, emptySplit, emptySplitWithZeroStoppingOffset)));

        // Fetch and check empty splits is added to finished splits
        RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> recordsWithSplitIds = reader.fetch();
        assertThat(recordsWithSplitIds.finishedSplits()).contains(emptySplit.splitId());
        assertThat(recordsWithSplitIds.finishedSplits())
                .contains(emptySplitWithZeroStoppingOffset.splitId());

        // Assign another valid split to avoid consumer.poll() blocking
        final PscTopicUriPartitionSplit anotherNormalSplit =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC_URI1, 1),
                        PscTopicUriPartitionSplit.EARLIEST_OFFSET,
                        PscTopicUriPartitionSplit.NO_STOPPING_OFFSET);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(anotherNormalSplit)));

        // Fetch again and check empty split set is cleared
        recordsWithSplitIds = reader.fetch();
        assertThat(recordsWithSplitIds.finishedSplits()).isEmpty();
    }

    @Test
    public void testUsingCommittedOffsetsWithNoneOffsetResetStrategy() throws ConfigurationException, ClientException {
        final Properties props = new Properties();
        props.setProperty(
                PscConfiguration.PSC_CONSUMER_GROUP_ID, "using-committed-offset-with-none-offset-reset");
        PscTopicUriPartitionSplitReader reader =
                createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());
        // We expect that there is a committed offset, but the group does not actually have a
        // committed offset, and the offset reset strategy is none (Throw exception to the consumer
        // if no previous offset is found for the consumer's group);
        // So it is expected to throw an exception that missing the committed offset.
        assertThatThrownBy(
                        () ->
                                reader.handleSplitsChanges(
                                        new SplitsAddition<>(
                                                Collections.singletonList(
                                                        new PscTopicUriPartitionSplit(
                                                                new TopicUriPartition(TOPIC_URI1, 0),
                                                                PscTopicUriPartitionSplit
                                                                        .COMMITTED_OFFSET)))))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(NoOffsetForPartitionException.class)
                .hasMessageContaining("Undefined offset with no reset policy for partition");
    }

    @ParameterizedTest
    @CsvSource({"earliest, 0", "latest, 10"})
    public void testUsingCommittedOffsetsWithEarliestOrLatestOffsetResetStrategy(
            String offsetResetStrategy, Long expectedOffset) throws ClientException, ConfigurationException {
        final Properties props = new Properties();
        props.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, offsetResetStrategy);
        props.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "using-committed-offset");
        PscTopicUriPartitionSplitReader reader =
                createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());
        // Add committed offset split
        final TopicUriPartition partition = new TopicUriPartition(TOPIC_URI1, 0);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(
                                        partition, PscTopicUriPartitionSplit.COMMITTED_OFFSET))));

        // Verify that the current offset of the consumer is the expected offset
        assertThat(reader.consumer().position(partition)).isEqualTo(expectedOffset);
    }

    @Test
    public void testConsumerClientRackSupplier() throws ConfigurationException, ClientException {
        String rackId = "use1-az1";
        Properties properties = new Properties();
        PscTopicUriPartitionSplitReader reader =
                createReader(
                        properties,
                        UnregisteredMetricsGroup.createSourceReaderMetricGroup(),
                        rackId);

        // Here we call the helper function directly, because the KafkaPartitionSplitReader
        // doesn't allow us to examine the final ConsumerConfig object
        reader.setConsumerClientRack(properties, rackId);
        assertThat(properties.get(ConsumerConfig.CLIENT_RACK_CONFIG)).isEqualTo(rackId);
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void testSetConsumerClientRackIgnoresNullAndEmpty(String rackId) throws ConfigurationException, ClientException {
        Properties properties = new Properties();
        PscTopicUriPartitionSplitReader reader =
                createReader(
                        properties,
                        UnregisteredMetricsGroup.createSourceReaderMetricGroup(),
                        rackId);

        // Here we call the helper function directly, because the KafkaPartitionSplitReader
        // doesn't allow us to examine the final ConsumerConfig object
        reader.setConsumerClientRack(properties, rackId);
        assertThat(properties.containsKey(ConsumerConfig.CLIENT_RACK_CONFIG)).isFalse();
    }

    // ------------------

    private void assignSplitsAndFetchUntilFinish(PscTopicUriPartitionSplitReader reader, int readerId)
            throws IOException, DeserializerException {
        Map<String, PscTopicUriPartitionSplit> splits =
                assignSplits(reader, splitsByOwners.get(readerId));

        Map<String, Integer> numConsumedRecords = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();
        while (finishedSplits.size() < splits.size()) {
            RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> recordsBySplitIds = reader.fetch();
            String splitId = recordsBySplitIds.nextSplit();
            while (splitId != null) {
                // Collect the records in this split.
                List<PscConsumerMessage<byte[], byte[]>> splitFetch = new ArrayList<>();
                PscConsumerMessage<byte[], byte[]> record;
                while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
                    splitFetch.add(record);
                }

                // Compute the expected next offset for the split.
                TopicUriPartition tp = splits.get(splitId).getTopicUriPartition();
                long earliestOffset = earliestOffsets.get(tp);
                int numConsumedRecordsForSplit = numConsumedRecords.getOrDefault(splitId, 0);
                long expectedStartingOffset = earliestOffset + numConsumedRecordsForSplit;

                // verify the consumed records.
                if (verifyConsumed(splits.get(splitId), expectedStartingOffset, splitFetch)) {
                    finishedSplits.add(splitId);
                }
                numConsumedRecords.compute(
                        splitId,
                        (ignored, recordCount) ->
                                recordCount == null
                                        ? splitFetch.size()
                                        : recordCount + splitFetch.size());
                splitId = recordsBySplitIds.nextSplit();
            }
        }

        // Verify the number of records consumed from each split.
        numConsumedRecords.forEach(
                (splitId, recordCount) -> {
                    TopicUriPartition tp = splits.get(splitId).getTopicUriPartition();
                    long earliestOffset = earliestOffsets.get(tp);
                    long expectedRecordCount = NUM_RECORDS_PER_PARTITION - earliestOffset;
                    assertThat((long) recordCount)
                            .as(
                                    String.format(
                                            "%s should have %d records.",
                                            splits.get(splitId), expectedRecordCount))
                            .isEqualTo(expectedRecordCount);
                });
    }

    // ------------------

    private PscTopicUriPartitionSplitReader createReader() throws ConfigurationException, ClientException {
        return createReader(
                new Properties(), UnregisteredMetricsGroup.createSourceReaderMetricGroup());
    }

    private PscTopicUriPartitionSplitReader createReader(
            Properties additionalProperties, SourceReaderMetricGroup sourceReaderMetricGroup) throws ConfigurationException, ClientException {
        return createReader(additionalProperties, sourceReaderMetricGroup, null);
    }

    private PscTopicUriPartitionSplitReader createReader(
            Properties additionalProperties,
            SourceReaderMetricGroup sourceReaderMetricGroup,
            String rackId) throws ConfigurationException, ClientException {
        Properties props = new Properties();
        props.putAll(PscSourceTestEnv.getConsumerProperties(ByteArrayDeserializer.class));
        props.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, "none");
        if (!additionalProperties.isEmpty()) {
            props.putAll(additionalProperties);
        }
        PscSourceReaderMetrics pscSourceReaderMetrics =
                new PscSourceReaderMetrics(sourceReaderMetricGroup);
        return new PscTopicUriPartitionSplitReader(
                props,
                new TestingReaderContext(new Configuration(), sourceReaderMetricGroup),
                pscSourceReaderMetrics,
                rackId);
    }

    private Map<String, PscTopicUriPartitionSplit> assignSplits(
            PscTopicUriPartitionSplitReader reader, Map<String, PscTopicUriPartitionSplit> splits) {
        SplitsChange<PscTopicUriPartitionSplit> splitsChange =
                new SplitsAddition<>(new ArrayList<>(splits.values()));
        reader.handleSplitsChanges(splitsChange);
        return splits;
    }

    private boolean verifyConsumed(
            final PscTopicUriPartitionSplit split,
            final long expectedStartingOffset,
            final Collection<PscConsumerMessage<byte[], byte[]>> consumed) throws DeserializerException {
        long expectedOffset = expectedStartingOffset;

        for (PscConsumerMessage<byte[], byte[]> record : consumed) {
            int expectedValue = (int) expectedOffset;
            long expectedTimestamp = expectedOffset * 1000L;

            assertThat(deserializer.deserialize(record.getValue())).isEqualTo(expectedValue);
            assertThat(record.getMessageId().getOffset()).isEqualTo(expectedOffset);
            assertThat(record.getMessageId().getTimestamp()).isEqualTo(expectedTimestamp);

            expectedOffset++;
        }
        if (split.getStoppingOffset().isPresent()) {
            return expectedOffset == split.getStoppingOffset().get();
        } else {
            return false;
        }
    }
}
