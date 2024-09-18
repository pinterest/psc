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
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.exception.consumer.ConsumerException;
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

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link PscTopicUriPartitionSplitReader}. */
public class PscTopicUriPartitionSplitReaderTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";

    private static Map<Integer, Map<String, PscTopicUriPartitionSplit>> splitsByOwners;
    private static Map<TopicUriPartition, Long> earliestOffsets;

    private final IntegerDeserializer deserializer = new IntegerDeserializer();

    @BeforeAll
    public static void setup() throws Throwable {
        PscSourceTestEnv.setup();
        PscSourceTestEnv.setupTopic(TOPIC1, true, true, PscSourceTestEnv::getRecordsForTopic);
        PscSourceTestEnv.setupTopic(TOPIC2, true, true, PscSourceTestEnv::getRecordsForTopic);
        splitsByOwners =
                PscSourceTestEnv.getSplitsByOwners(Arrays.asList(TOPIC1, TOPIC2), NUM_SUBTASKS);
        earliestOffsets =
                PscSourceTestEnv.getEarliestOffsets(
                        PscSourceTestEnv.getPartitionsForTopics(Arrays.asList(TOPIC1, TOPIC2)));
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
        TopicUriPartition nonExistingTopicPartition = new TopicUriPartition("NotExist", 0);
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
        assertNull(error.get());
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
        TopicUriPartition tp = new TopicUriPartition(TOPIC1, 0);
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
        PscTopicUriPartitionSplitReader reader =
                createReader(
                        new Properties(),
                        InternalSourceReaderMetricGroup.wrap(operatorMetricGroup));
        // Add a split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC1, 0), 0L))));
        reader.fetch();
        final long latestNumBytesIn = numBytesInCounter.getCount();
        // Since it's hard to know the exact number of bytes consumed, we just check if it is
        // greater than 0
        assertThat(latestNumBytesIn, Matchers.greaterThan(0L));
        // Add another split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(new TopicUriPartition(TOPIC2, 0), 0L))));
        reader.fetch();
        // We just check if numBytesIn is increasing
        assertThat(numBytesInCounter.getCount(), Matchers.greaterThan(latestNumBytesIn));
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"_underscore.period-minus"})
    public void testPendingRecordsGauge(String topicSuffix) throws Throwable {
        final String topic1Name = TOPIC1 + topicSuffix;
        final String topic2Name = TOPIC2 + topicSuffix;
        if (!topicSuffix.isEmpty()) {
            PscSourceTestEnv.setupTopic(
                    topic1Name, true, true, PscSourceTestEnv::getRecordsForTopic);
            PscSourceTestEnv.setupTopic(
                    topic2Name, true, true, PscSourceTestEnv::getRecordsForTopic);
        }
        MetricListener metricListener = new MetricListener();
        final Properties props = new Properties();
        props.setProperty(PscConfiguration.PSC_CONSUMER_POLL_MESSAGES_MAX, "1");
        PscTopicUriPartitionSplitReader reader =
                createReader(
                        props,
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        // Add a split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(new TopicUriPartition(topic1Name, 0), 0L))));
        // pendingRecords should have not been registered because of lazily registration
        assertFalse(metricListener.getGauge(MetricNames.PENDING_RECORDS).isPresent());
        // Trigger first fetch
        reader.fetch();
        final Optional<Gauge<Long>> pendingRecords =
                metricListener.getGauge(MetricNames.PENDING_RECORDS);
        assertTrue(pendingRecords.isPresent());
        // Validate pendingRecords
        assertNotNull(pendingRecords);
        assertEquals(NUM_RECORDS_PER_PARTITION - 1, (long) pendingRecords.get().getValue());
        for (int i = 1; i < NUM_RECORDS_PER_PARTITION; i++) {
            reader.fetch();
            assertEquals(NUM_RECORDS_PER_PARTITION - i - 1, (long) pendingRecords.get().getValue());
        }
        // Add another split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(new TopicUriPartition(topic2Name, 0), 0L))));
        // Validate pendingRecords
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            reader.fetch();
            assertEquals(NUM_RECORDS_PER_PARTITION - i - 1, (long) pendingRecords.get().getValue());
        }
    }

    @Test
    public void testAssignEmptySplit() throws Exception {
        PscTopicUriPartitionSplitReader reader = createReader();
        final PscTopicUriPartitionSplit normalSplit =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC1, 0),
                        PscTopicUriPartitionSplit.EARLIEST_OFFSET,
                        PscTopicUriPartitionSplit.NO_STOPPING_OFFSET);
        final PscTopicUriPartitionSplit emptySplit =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC2, 0),
                        PscTopicUriPartitionSplit.LATEST_OFFSET,
                        PscTopicUriPartitionSplit.LATEST_OFFSET);
        reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(normalSplit, emptySplit)));

        // Fetch and check empty splits is added to finished splits
        RecordsWithSplitIds<PscConsumerMessage<byte[], byte[]>> recordsWithSplitIds = reader.fetch();
        assertTrue(recordsWithSplitIds.finishedSplits().contains(emptySplit.splitId()));

        // Assign another valid split to avoid consumer.poll() blocking
        final PscTopicUriPartitionSplit anotherNormalSplit =
                new PscTopicUriPartitionSplit(
                        new TopicUriPartition(TOPIC1, 1),
                        PscTopicUriPartitionSplit.EARLIEST_OFFSET,
                        PscTopicUriPartitionSplit.NO_STOPPING_OFFSET);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(anotherNormalSplit)));

        // Fetch again and check empty split set is cleared
        recordsWithSplitIds = reader.fetch();
        assertTrue(recordsWithSplitIds.finishedSplits().isEmpty());
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
        final ConsumerException undefinedOffsetException =
                Assertions.assertThrows(
                        ConsumerException.class,
                        () ->
                                reader.handleSplitsChanges(
                                        new SplitsAddition<>(
                                                Collections.singletonList(
                                                        new PscTopicUriPartitionSplit(
                                                                new TopicUriPartition(TOPIC1, 0),
                                                                PscTopicUriPartitionSplit
                                                                        .COMMITTED_OFFSET)))));
        MatcherAssert.assertThat(
                undefinedOffsetException.getMessage(),
                CoreMatchers.containsString("Undefined offset with no reset policy for partition"));
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
        final TopicUriPartition partition = new TopicUriPartition(TOPIC1, 0);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new PscTopicUriPartitionSplit(
                                        partition, PscTopicUriPartitionSplit.COMMITTED_OFFSET))));

        // Verify that the current offset of the consumer is the expected offset
        assertEquals(expectedOffset, reader.consumer().position(partition));
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
                    assertEquals(
                            expectedRecordCount,
                            (long) recordCount,
                            String.format(
                                    "%s should have %d records.",
                                    splits.get(splitId), expectedRecordCount));
                });
    }

    // ------------------

    private PscTopicUriPartitionSplitReader createReader() throws ConfigurationException, ClientException {
        return createReader(
                new Properties(), UnregisteredMetricsGroup.createSourceReaderMetricGroup());
    }

    private PscTopicUriPartitionSplitReader createReader(
            Properties additionalProperties, SourceReaderMetricGroup sourceReaderMetricGroup) throws ConfigurationException, ClientException {
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
                pscSourceReaderMetrics);
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

            assertEquals(expectedValue, deserializer.deserialize(record.getValue()));
            assertEquals(expectedOffset, record.getMessageId().getOffset());
            assertEquals(expectedTimestamp, record.getMessageId().getTimestamp());

            expectedOffset++;
        }
        if (split.getStoppingOffset().isPresent()) {
            return expectedOffset == split.getStoppingOffset().get();
        } else {
            return false;
        }
    }
}
