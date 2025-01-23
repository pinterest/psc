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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.source.enumerator.AssignmentStatus;
import com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumState;
import com.pinterest.flink.connector.psc.source.enumerator.TopicUriPartitionAndAssignmentStatus;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test {@link DynamicPscSourceEnumStateSerializer}.
 */
public class DynamicPscSourceEnumStateSerializerTest {

    @Test
    public void testSerde() throws Exception {
        DynamicPscSourceEnumStateSerializer dynamicPscSourceEnumStateSerializer =
                new DynamicPscSourceEnumStateSerializer();

        Properties propertiesForCluster0 = new Properties();
        propertiesForCluster0.setProperty(
                PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        Properties propertiesForCluster1 = new Properties();
        propertiesForCluster1.setProperty(
                PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);

        Set<PscStream> pscStreams =
                ImmutableSet.of(
                        new PscStream(
                                "stream0",
                                ImmutableMap.of(
                                        "cluster0",
                                        new ClusterMetadata(
                                                ImmutableSet.of("topic0", "topic1"),
                                                propertiesForCluster0),
                                        "cluster1",
                                        new ClusterMetadata(
                                                ImmutableSet.of("topic2", "topic3"),
                                                propertiesForCluster1))),
                        new PscStream(
                                "stream1",
                                ImmutableMap.of(
                                        "cluster1",
                                        new ClusterMetadata(
                                                ImmutableSet.of("topic4", "topic5"),
                                                propertiesForCluster1))));

        DynamicPscSourceEnumState dynamicPscSourceEnumState =
                new DynamicPscSourceEnumState(
                        pscStreams,
                        ImmutableMap.of(
                                "cluster0",
                                new PscSourceEnumState(
                                        ImmutableSet.of(
                                                new TopicUriPartitionAndAssignmentStatus(
                                                        new TopicUriPartition("topic0", 0),
                                                        AssignmentStatus.ASSIGNED),
                                                new TopicUriPartitionAndAssignmentStatus(
                                                        new TopicUriPartition("topic1", 1),
                                                        AssignmentStatus.UNASSIGNED_INITIAL)),
                                        true),
                                "cluster1",
                                new PscSourceEnumState(
                                        ImmutableSet.of(
                                                new TopicUriPartitionAndAssignmentStatus(
                                                        new TopicUriPartition("topic2", 0),
                                                        AssignmentStatus.UNASSIGNED_INITIAL),
                                                new TopicUriPartitionAndAssignmentStatus(
                                                        new TopicUriPartition("topic3", 1),
                                                        AssignmentStatus.UNASSIGNED_INITIAL),
                                                new TopicUriPartitionAndAssignmentStatus(
                                                        new TopicUriPartition("topic4", 2),
                                                        AssignmentStatus.UNASSIGNED_INITIAL),
                                                new TopicUriPartitionAndAssignmentStatus(
                                                        new TopicUriPartition("topic5", 3),
                                                        AssignmentStatus.UNASSIGNED_INITIAL)),
                                        false)));

        DynamicPscSourceEnumState dynamicPscSourceEnumStateAfterSerde =
                dynamicPscSourceEnumStateSerializer.deserialize(
                        dynamicPscSourceEnumStateSerializer.getVersion(),
                        dynamicPscSourceEnumStateSerializer.serialize(
                                dynamicPscSourceEnumState));

        assertThat(dynamicPscSourceEnumState)
                .usingRecursiveComparison()
                .isEqualTo(dynamicPscSourceEnumStateAfterSerde);
    }
}
