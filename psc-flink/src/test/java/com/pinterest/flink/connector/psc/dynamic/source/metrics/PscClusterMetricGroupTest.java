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

package com.pinterest.flink.connector.psc.dynamic.source.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.pinterest.flink.connector.psc.dynamic.source.metrics.PscClusterMetricGroup.DYNAMIC_PSC_SOURCE_METRIC_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test for {@link
 * org.apache.flink.connector.kafka.dynamic.source.metrics.KafkaClusterMetricGroup}.
 */
public class PscClusterMetricGroupTest {

    private static MetricListener metricListener;
    private static InternalSourceReaderMetricGroup mockInternalSourceReaderMetricGroup;
    private static PscClusterMetricGroup pscClusterMetricGroup;

    @BeforeEach
    public void beforeEach() {
        metricListener = new MetricListener();
        mockInternalSourceReaderMetricGroup =
                InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup());
        pscClusterMetricGroup =
                new PscClusterMetricGroup(
                        mockInternalSourceReaderMetricGroup.addGroup(
                                DYNAMIC_PSC_SOURCE_METRIC_GROUP),
                        mockInternalSourceReaderMetricGroup,
                        "broker-bootstrap-server:443");
    }

    @Test
    public void testGetAllVariables() {
        // metric variables are wrapped in <...>
        assertThat(pscClusterMetricGroup.getAllVariables())
                .as(
                        "variables should contain Kafka cluster info to distinguish multiple sub KafkaSourceReaders")
                .containsEntry(
                        "<" + PscClusterMetricGroup.PSC_CLUSTER_GROUP_NAME + ">",
                        "broker-bootstrap-server:443");
    }

    @Test
    public void testGetScopeComponents() {
        assertThat(pscClusterMetricGroup.getScopeComponents())
                .as("scope components contains previously attached scope component")
                .contains(DYNAMIC_PSC_SOURCE_METRIC_GROUP);
    }

    @Test
    public void testSetPendingRecordsGauge() {
        pscClusterMetricGroup.setPendingRecordsGauge(() -> 5L);

        // these identifiers should be attached to distinguish distinguish multiple sub
        // KafkaSourceReaders
        Optional<Gauge<Long>> pendingRecordsGauge =
                metricListener.getGauge(
                        DYNAMIC_PSC_SOURCE_METRIC_GROUP,
                        "pscCluster",
//                        "broker-bootstrap-server:443",
                        "pendingRecords");

        assertThat(pendingRecordsGauge.get().getValue()).isEqualTo(5L);
    }

    @Test
    public void testGetIOMetricGroup() {
        assertThat(pscClusterMetricGroup.getIOMetricGroup())
                .isEqualTo(mockInternalSourceReaderMetricGroup.getIOMetricGroup());
    }
}
