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

import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import org.junit.Test;

import java.util.Optional;

import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.PARTITION_GROUP;
import static com.pinterest.flink.connector.psc.source.metrics.PscSourceReaderMetrics.TOPIC_URI_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link PscSourceReaderMetrics}. */
public class PscSourceReaderMetricsTest {

    private static final TopicUriPartition FOO_0 = new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "foo", 0);
    private static final TopicUriPartition FOO_1 = new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "foo", 1);
    private static final TopicUriPartition BAR_0 = new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "bar", 0);
    private static final TopicUriPartition BAR_1 = new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "bar", 1);

    @Test
    public void testCurrentOffsetTracking() {
        MetricListener metricListener = new MetricListener();

        final PscSourceReaderMetrics pscSourceReaderMetrics =
                new PscSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        pscSourceReaderMetrics.registerTopicUriPartition(FOO_0);
        pscSourceReaderMetrics.registerTopicUriPartition(FOO_1);
        pscSourceReaderMetrics.registerTopicUriPartition(BAR_0);
        pscSourceReaderMetrics.registerTopicUriPartition(BAR_1);

        pscSourceReaderMetrics.recordCurrentOffset(FOO_0, 15213L);
        pscSourceReaderMetrics.recordCurrentOffset(FOO_1, 18213L);
        pscSourceReaderMetrics.recordCurrentOffset(BAR_0, 18613L);
        pscSourceReaderMetrics.recordCurrentOffset(BAR_1, 15513L);

        assertCurrentOffset(FOO_0, 15213L, metricListener);
        assertCurrentOffset(FOO_1, 18213L, metricListener);
        assertCurrentOffset(BAR_0, 18613L, metricListener);
        assertCurrentOffset(BAR_1, 15513L, metricListener);
    }

    @Test
    public void testCommitOffsetTracking() {
        MetricListener metricListener = new MetricListener();

        final PscSourceReaderMetrics pscSourceReaderMetrics =
                new PscSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        pscSourceReaderMetrics.registerTopicUriPartition(FOO_0);
        pscSourceReaderMetrics.registerTopicUriPartition(FOO_1);
        pscSourceReaderMetrics.registerTopicUriPartition(BAR_0);
        pscSourceReaderMetrics.registerTopicUriPartition(BAR_1);

        pscSourceReaderMetrics.recordCommittedOffset(FOO_0, 15213L);
        pscSourceReaderMetrics.recordCommittedOffset(FOO_1, 18213L);
        pscSourceReaderMetrics.recordCommittedOffset(BAR_0, 18613L);
        pscSourceReaderMetrics.recordCommittedOffset(BAR_1, 15513L);

        assertCommittedOffset(FOO_0, 15213L, metricListener);
        assertCommittedOffset(FOO_1, 18213L, metricListener);
        assertCommittedOffset(BAR_0, 18613L, metricListener);
        assertCommittedOffset(BAR_1, 15513L, metricListener);

        final Optional<Counter> commitsSucceededCounter =
                metricListener.getCounter(
                        PscSourceReaderMetrics.PSC_SOURCE_READER_METRIC_GROUP,
                        PscSourceReaderMetrics.COMMITS_SUCCEEDED_METRIC_COUNTER);
        assertThat(commitsSucceededCounter).isPresent();
        assertThat(commitsSucceededCounter.get().getCount()).isEqualTo(0L);

        pscSourceReaderMetrics.recordSucceededCommit();

        assertThat(commitsSucceededCounter.get().getCount()).isEqualTo(1L);
    }

    @Test
    public void testNonTrackingTopicPartition() {
        MetricListener metricListener = new MetricListener();
        final PscSourceReaderMetrics pscSourceReaderMetrics =
                new PscSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        assertThatThrownBy(() -> pscSourceReaderMetrics.recordCurrentOffset(FOO_0, 15213L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pscSourceReaderMetrics.recordCommittedOffset(FOO_0, 15213L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testFailedCommit() {
        MetricListener metricListener = new MetricListener();
        final PscSourceReaderMetrics pscSourceReaderMetrics =
                new PscSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        pscSourceReaderMetrics.recordFailedCommit();
        final Optional<Counter> commitsFailedCounter =
                metricListener.getCounter(
                        PscSourceReaderMetrics.PSC_SOURCE_READER_METRIC_GROUP,
                        PscSourceReaderMetrics.COMMITS_FAILED_METRIC_COUNTER);
        assertThat(commitsFailedCounter).isPresent();
        assertThat(commitsFailedCounter.get().getCount()).isEqualTo(1L);
    }

    // ----------- Assertions --------------

    private void assertCurrentOffset(
            TopicUriPartition tp, long expectedOffset, MetricListener metricListener) {
        final Optional<Gauge<Long>> currentOffsetGauge =
                metricListener.getGauge(
                        PscSourceReaderMetrics.PSC_SOURCE_READER_METRIC_GROUP,
                        TOPIC_URI_GROUP,
                        tp.getTopicUriAsString(),
                        PARTITION_GROUP,
                        String.valueOf(tp.getPartition()),
                        PscSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE);
        assertThat(currentOffsetGauge).isPresent();
        assertThat((long) currentOffsetGauge.get().getValue()).isEqualTo(expectedOffset);
    }

    private void assertCommittedOffset(
            TopicUriPartition tp, long expectedOffset, MetricListener metricListener) {
        final Optional<Gauge<Long>> committedOffsetGauge =
                metricListener.getGauge(
                        PscSourceReaderMetrics.PSC_SOURCE_READER_METRIC_GROUP,
                        TOPIC_URI_GROUP,
                        tp.getTopicUriAsString(),
                        PARTITION_GROUP,
                        String.valueOf(tp.getPartition()),
                        PscSourceReaderMetrics.COMMITTED_OFFSET_METRIC_GAUGE);
        assertThat(committedOffsetGauge).isPresent();
        assertThat((long) committedOffsetGauge.get().getValue()).isEqualTo(expectedOffset);
    }
}
