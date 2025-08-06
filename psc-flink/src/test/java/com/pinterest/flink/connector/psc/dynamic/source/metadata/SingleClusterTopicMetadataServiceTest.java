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

package com.pinterest.flink.connector.psc.dynamic.source.metadata;

import com.google.common.collect.ImmutableMap;
import com.pinterest.flink.connector.psc.dynamic.metadata.ClusterMetadata;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.metadata.PscStream;
import com.pinterest.flink.connector.psc.dynamic.metadata.SingleClusterTopicUriMetadataService;
import com.pinterest.flink.streaming.connectors.psc.DynamicPscSourceTestHelperWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.PscTestBaseWithKafkaAsPubSub;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class SingleClusterTopicMetadataServiceTest {

    private static final String TOPIC0 = "SingleClusterTopicMetadataServiceTest-1";
    private static final String TOPIC1 = "SingleClusterTopicMetadataServiceTest-2";

    private static PscMetadataService pscMetadataService;
    private static PscTestBaseWithKafkaAsPubSub.ClusterTestEnvMetadata kafkaClusterTestEnvMetadata0;

    @BeforeAll
    static void beforeAll() throws Throwable {
        DynamicPscSourceTestHelperWithKafkaAsPubSub.setup();
        DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(TOPIC0, 3);
        DynamicPscSourceTestHelperWithKafkaAsPubSub.createTopic(TOPIC1, 3);

        kafkaClusterTestEnvMetadata0 =
                DynamicPscSourceTestHelperWithKafkaAsPubSub.getClusterTestEnvMetadata(0);

        pscMetadataService =
                new SingleClusterTopicUriMetadataService(
                        kafkaClusterTestEnvMetadata0.getPubSubClusterId(),
                        kafkaClusterTestEnvMetadata0.getStandardProperties());
    }

    @AfterAll
    static void afterAll() throws Exception {
        DynamicPscSourceTestHelperWithKafkaAsPubSub.tearDown();
    }

    @Test
    void getAllStreams() {
        Set<PscStream> allStreams = pscMetadataService.getAllStreams();
        assertThat(allStreams)
                .as("stream names should be equal to topic names")
                .containsExactlyInAnyOrder(
                        new PscStream(
                                TOPIC0,
                                ImmutableMap.of(
                                        kafkaClusterTestEnvMetadata0.getPubSubClusterId(),
                                        new ClusterMetadata(
                                                Collections.singleton(TOPIC0),
                                                kafkaClusterTestEnvMetadata0
                                                        .getStandardProperties()))),
                        new PscStream(
                                TOPIC1,
                                Collections.singletonMap(
                                        kafkaClusterTestEnvMetadata0.getPubSubClusterId(),
                                        new ClusterMetadata(
                                                Collections.singleton(TOPIC1),
                                                kafkaClusterTestEnvMetadata0
                                                        .getStandardProperties()))));
    }

    @Test
    void describeStreams() {
        Map<String, PscStream> streamMap =
                pscMetadataService.describeStreams(Collections.singleton(TOPIC1));
        assertThat(streamMap)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                TOPIC1,
                                new PscStream(
                                        TOPIC1,
                                        Collections.singletonMap(
                                                kafkaClusterTestEnvMetadata0.getPubSubClusterId(),
                                                new ClusterMetadata(
                                                        Collections.singleton(TOPIC1),
                                                        kafkaClusterTestEnvMetadata0
                                                                .getStandardProperties())))));

        assertThatCode(
                        () ->
                                pscMetadataService.describeStreams(
                                        Collections.singleton("unknown-stream")))
                .as("the stream topic cannot be found in kafka and we rethrow")
                .hasRootCauseInstanceOf(UnknownTopicOrPartitionException.class);
    }
}
