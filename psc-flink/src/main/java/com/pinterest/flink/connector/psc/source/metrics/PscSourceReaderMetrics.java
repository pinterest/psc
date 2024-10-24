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

package com.pinterest.flink.connector.psc.source.metrics;

import com.pinterest.flink.connector.psc.MetricUtil;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.exception.ClientException;
import com.pinterest.psc.metrics.Metric;
import com.pinterest.psc.metrics.MetricName;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * A collection class for handling metrics in {@link com.pinterest.flink.connector.psc.source.reader.PscSourceReader}.
 *
 * <p>All metrics of Kafka source reader are registered under group "KafkaSourceReader", which is a
 * child group of {@link org.apache.flink.metrics.groups.OperatorMetricGroup}. Metrics related to a
 * specific topic partition will be registered in the group
 * "KafkaSourceReader.topic.{topic_name}.partition.{partition_id}".
 *
 * <p>For example, current consuming offset of topic "my-topic" and partition 1 will be reported in
 * metric:
 * "{some_parent_groups}.operator.KafkaSourceReader.topic.my-topic.partition.1.currentOffset"
 *
 * <p>and number of successful commits will be reported in metric:
 * "{some_parent_groups}.operator.KafkaSourceReader.commitsSucceeded"
 *
 * <p>All metrics of Kafka consumer are also registered under group
 * "KafkaSourceReader.KafkaConsumer". For example, Kafka consumer metric "records-consumed-total"
 * can be found at:
 * {some_parent_groups}.operator.KafkaSourceReader.KafkaConsumer.records-consumed-total"
 */
@PublicEvolving
public class PscSourceReaderMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(PscSourceReaderMetrics.class);

    // Constants
    public static final String PSC_SOURCE_READER_METRIC_GROUP = "PscSourceReader";
    public static final String TOPIC_URI_GROUP = "topicUri";
    public static final String PARTITION_GROUP = "partition";
    public static final String CURRENT_OFFSET_METRIC_GAUGE = "currentOffset";
    public static final String COMMITTED_OFFSET_METRIC_GAUGE = "committedOffset";
    public static final String COMMITS_SUCCEEDED_METRIC_COUNTER = "commitsSucceeded";
    public static final String COMMITS_FAILED_METRIC_COUNTER = "commitsFailed";
    public static final String PSC_CONSUMER_METRIC_GROUP = "PscConsumer";

    public static final long INITIAL_OFFSET = -1;

    // Source reader metric group
    private final SourceReaderMetricGroup sourceReaderMetricGroup;

    // Metric group for registering Kafka specific metrics
    private final MetricGroup pscSourceReaderMetricGroup;

    // Successful / Failed commits counters
    private final Counter commitsSucceeded;
    private final Counter commitsFailed;

    // Map for tracking current consuming / committing offsets
    private final Map<TopicUriPartition, Offset> offsets = new HashMap<>();

    // Map for tracking records lag of topic partitions
    @Nullable private ConcurrentMap<TopicUriPartition, Metric> recordsLagMetrics;

    // Kafka raw metric for bytes consumed total
    @Nullable private Metric bytesConsumedTotalMetric;

    /** Number of bytes consumed total at the latest {@link #updateNumBytesInCounter()}. */
    private long latestBytesConsumedTotal;

    public PscSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
        this.pscSourceReaderMetricGroup =
                sourceReaderMetricGroup.addGroup(PSC_SOURCE_READER_METRIC_GROUP);
        this.commitsSucceeded =
                this.pscSourceReaderMetricGroup.counter(COMMITS_SUCCEEDED_METRIC_COUNTER);
        this.commitsFailed =
                this.pscSourceReaderMetricGroup.counter(COMMITS_FAILED_METRIC_COUNTER);
    }

    /**
     * Register metrics of KafkaConsumer in Kafka metric group.
     *
     * @param pscConsumer Kafka consumer used by partition split reader.
     */
    @SuppressWarnings("Convert2MethodRef")
    public void registerPscConsumerMetrics(PscConsumer<?, ?> pscConsumer) throws ClientException {
        final Map<MetricName, ? extends Metric> pscConsumerMetrics = pscConsumer.metrics();
        if (pscConsumerMetrics == null) {
            LOG.warn("Consumer implementation does not support metrics");
            return;
        }

        final MetricGroup pscConsumerMetricGroup =
                pscSourceReaderMetricGroup.addGroup(PSC_CONSUMER_METRIC_GROUP);

        pscConsumerMetrics.forEach(
                (name, metric) ->
                        pscConsumerMetricGroup.gauge(name.name(), () -> metric.metricValue()));
    }

    /**
     * Register metric groups for the given {@link TopicUriPartition}.
     *
     * @param tp Registering topic partition
     */
    public void registerTopicUriPartition(TopicUriPartition tp) {
        offsets.put(tp, new Offset(INITIAL_OFFSET, INITIAL_OFFSET));
        registerOffsetMetricsForTopicPartition(tp);
    }

    /**
     * Update current consuming offset of the given {@link TopicUriPartition}.
     *
     * @param tp Updating topic partition
     * @param offset Current consuming offset
     */
    public void recordCurrentOffset(TopicUriPartition tp, long offset) {
        checkTopicPartitionTracked(tp);
        offsets.get(tp).currentOffset = offset;
    }

    /**
     * Update the latest committed offset of the given {@link TopicUriPartition}.
     *
     * @param tp Updating topic partition
     * @param offset Committing offset
     */
    public void recordCommittedOffset(TopicUriPartition tp, long offset) {
        checkTopicPartitionTracked(tp);
        offsets.get(tp).committedOffset = offset;
    }

    /** Mark a successful commit. */
    public void recordSucceededCommit() {
        commitsSucceeded.inc();
    }

    /** Mark a failure commit. */
    public void recordFailedCommit() {
        commitsFailed.inc();
    }

    /**
     * Register {@link MetricNames#IO_NUM_BYTES_IN}.
     *
     * @param consumer Kafka consumer
     */
    public void registerNumBytesIn(PscConsumer<?, ?> consumer) throws ClientException {
        Predicate<Map.Entry<MetricName, ? extends Metric>> filter =
                KafkaSourceReaderMetricsUtil.createBytesConsumedFilter();
        this.bytesConsumedTotalMetric = MetricUtil.getPscMetric(consumer.metrics(), filter);
    }

    /**
     * Add a partition's records-lag metric to tracking list if this partition never appears before.
     *
     * <p>This method also lazily register {@link
     * MetricNames#PENDING_RECORDS} in {@link
     * SourceReaderMetricGroup}
     *
     * @param consumer Kafka consumer
     * @param tp Topic partition
     */
    public void maybeAddRecordsLagMetric(PscConsumer<?, ?> consumer, TopicUriPartition tp) {
        // Lazily register pendingRecords
        if (recordsLagMetrics == null) {
            this.recordsLagMetrics = new ConcurrentHashMap<>();
            this.sourceReaderMetricGroup.setPendingRecordsGauge(
                    () -> {
                        long pendingRecordsTotal = 0;
                        for (Metric recordsLagMetric : this.recordsLagMetrics.values()) {
                            pendingRecordsTotal +=
                                    ((Double) recordsLagMetric.metricValue()).longValue();
                        }
                        return pendingRecordsTotal;
                    });
        }
        recordsLagMetrics.computeIfAbsent(
                tp, (ignored) -> {
                    try {
                        return getRecordsLagMetric(consumer.metrics(), tp);
                    } catch (ClientException e) {
                        throw new RuntimeException("Failed to get consumer metrics", e);
                    }
                });
    }

    /**
     * Remove a partition's records-lag metric from tracking list.
     *
     * @param tp Unassigned topic partition
     */
    public void removeRecordsLagMetric(TopicUriPartition tp) {
        if (recordsLagMetrics != null) {
            recordsLagMetrics.remove(tp);
        }
    }

    /**
     * Update {@link MetricNames#IO_NUM_BYTES_IN}.
     *
     * <p>Instead of simply setting {@link OperatorIOMetricGroup#getNumBytesInCounter()} to the same
     * value as bytes-consumed-total from Kafka consumer, which will screw {@link
     * TaskIOMetricGroup#getNumBytesInCounter()} if chained sources exist, we track the increment of
     * bytes-consumed-total and count it towards the counter.
     */
    public void updateNumBytesInCounter() {
        if (this.bytesConsumedTotalMetric != null) {
            long bytesConsumedUntilNow =
                    ((Number) this.bytesConsumedTotalMetric.metricValue()).longValue();
            long bytesConsumedSinceLastUpdate = bytesConsumedUntilNow - latestBytesConsumedTotal;
            this.sourceReaderMetricGroup
                    .getIOMetricGroup()
                    .getNumBytesInCounter()
                    .inc(bytesConsumedSinceLastUpdate);
            latestBytesConsumedTotal = bytesConsumedUntilNow;
        }
    }

    // -------- Helper functions --------
    private void registerOffsetMetricsForTopicPartition(TopicUriPartition tp) {
        final MetricGroup topicPartitionGroup =
                this.pscSourceReaderMetricGroup
                        .addGroup(TOPIC_URI_GROUP, tp.getTopicUriAsString())
                        .addGroup(PARTITION_GROUP, String.valueOf(tp.getPartition()));
        topicPartitionGroup.gauge(
                CURRENT_OFFSET_METRIC_GAUGE,
                () ->
                        offsets.getOrDefault(tp, new Offset(INITIAL_OFFSET, INITIAL_OFFSET))
                                .currentOffset);
        topicPartitionGroup.gauge(
                COMMITTED_OFFSET_METRIC_GAUGE,
                () ->
                        offsets.getOrDefault(tp, new Offset(INITIAL_OFFSET, INITIAL_OFFSET))
                                .committedOffset);
    }

    private void checkTopicPartitionTracked(TopicUriPartition tp) {
        if (!offsets.containsKey(tp)) {
            throw new IllegalArgumentException(
                    String.format("TopicPartition %s is not tracked", tp));
        }
    }

    private @Nullable Metric getRecordsLagMetric(
            Map<MetricName, ? extends Metric> metrics, TopicUriPartition tp) {
        Predicate<Map.Entry<MetricName, ? extends Metric>> filter;
        String backendType = getBackendFromTags(metrics);
        switch (backendType) {
            case PscUtils.BACKEND_TYPE_KAFKA:
                filter = KafkaSourceReaderMetricsUtil.createRecordLagFilter(tp);
                break;
            default:
                LOG.warn(
                        String.format(
                                "Unsupported backend type \"%s\". "
                                        + "Metric \"%s\" may not be reported correctly. ",
                                backendType, MetricNames.PENDING_RECORDS));
                return null;
        }
        return MetricUtil.getPscMetric(metrics, filter);
    }

    private static String getBackendFromTags(Map<MetricName, ? extends Metric> metrics) {
        // sample the first entry to get the backend type
        return metrics.keySet().iterator().next().tags().get("backend");
    }

    private static class Offset {
        long currentOffset;
        long committedOffset;

        Offset(long currentOffset, long committedOffset) {
            this.currentOffset = currentOffset;
            this.committedOffset = committedOffset;
        }
    }
}
