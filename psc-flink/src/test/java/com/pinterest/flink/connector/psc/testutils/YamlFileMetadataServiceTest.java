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

package com.pinterest.flink.connector.psc.testutils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.pinterest.flink.connector.psc.PscFlinkConfiguration;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** A test class for {@link YamlFileMetadataService}. */
public class YamlFileMetadataServiceTest {

    @Test
    public void testParseFile() throws IOException {
        YamlFileMetadataService yamlFileMetadataService =
                new YamlFileMetadataService(
                        Resources.getResource("stream-metadata.yaml").getPath(), Duration.ZERO);
        Set<PscStream> kafkaStreams = yamlFileMetadataService.parseFile();

        Properties propertiesForCluster0 = new Properties();
        propertiesForCluster0.setProperty(
                PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        PscTestUtils.putDiscoveryProperties(propertiesForCluster0, "bootstrap-server-0:443", PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX);
        Properties propertiesForCluster1 = new Properties();
        propertiesForCluster1.setProperty(
                PscFlinkConfiguration.CLUSTER_URI_CONFIG, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX);
        PscTestUtils.putDiscoveryProperties(propertiesForCluster1, "bootstrap-server-1:443", PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX);

        Properties propertiesForCluster2 = new Properties();
        String cluster2Uri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX.replace("cluster1", "cluster2");
        propertiesForCluster2.setProperty(
                PscFlinkConfiguration.CLUSTER_URI_CONFIG, cluster2Uri);
        PscTestUtils.putDiscoveryProperties(propertiesForCluster2, "bootstrap-server-2:443", cluster2Uri);

        assertThat(kafkaStreams)
                .containsExactlyInAnyOrderElementsOf(
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
                                                "cluster2",
                                                new ClusterMetadata(
                                                        ImmutableSet.of("topic4", "topic5"),
                                                        propertiesForCluster2)))));
    }
}
