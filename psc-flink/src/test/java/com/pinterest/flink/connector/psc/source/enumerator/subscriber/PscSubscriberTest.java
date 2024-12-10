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

package com.pinterest.flink.connector.psc.source.enumerator.subscriber;

import com.pinterest.flink.connector.psc.testutils.PscSourceTestEnv;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.metadata.client.PscMetadataClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PscSubscriber}. */
public class PscSubscriberTest {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC_URI1 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + TOPIC1;
    private static final String TOPIC2 = "pattern-topic";
    private static final String TOPIC_URI2 = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + TOPIC2;
    private static final String TOPIC_URI1_SECURE = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX.replace("plaintext:/", "secure:/") + TOPIC1;
    private static final TopicUriPartition NON_EXISTING_TOPIC_URI_PARTITION = new TopicUriPartition(
            PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "removed",
            0);
    private static AdminClient adminClient;
    private static PscMetadataClient pscMetadataClient;
    private static PscMetadataClient pscMetadataClientSecure;

    @BeforeClass
    public static void setup() throws Throwable {
        PscSourceTestEnv.setup();
        PscSourceTestEnv.createTestTopic(TOPIC_URI1);
        PscSourceTestEnv.createTestTopic(TOPIC_URI2);
        adminClient = PscSourceTestEnv.getAdminClient();
        pscMetadataClient = PscSourceTestEnv.getMetadataClient();
        pscMetadataClientSecure = PscSourceTestEnv.getSecureMetadataClient();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        adminClient.close();
        PscSourceTestEnv.tearDown();
    }

    @Test
    public void testTopicUriListSubscriber() {
        List<String> topics = Arrays.asList(TOPIC_URI1, TOPIC_URI2);
        PscSubscriber subscriber =
                PscSubscriber.getTopicUriListSubscriber(Arrays.asList(TOPIC_URI1, TOPIC_URI2));
        final Set<TopicUriPartition> subscribedPartitions =
                subscriber.getSubscribedTopicUriPartitions(pscMetadataClient, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI);

        final Set<TopicUriPartition> expectedSubscribedPartitions =
                new HashSet<>(PscSourceTestEnv.getPartitionsForTopics(topics));

        assertThat(subscribedPartitions).isEqualTo(expectedSubscribedPartitions);
    }

    @Test
    public void testTopicUriListSubscriberPreservesProtocol() {
        PscSubscriber subscriber =
                PscSubscriber.getTopicUriListSubscriber(Arrays.asList(TOPIC_URI1_SECURE, TOPIC_URI2));
        final Set<TopicUriPartition> subscribedPartitionsPlaintext =
                subscriber.getSubscribedTopicUriPartitions(pscMetadataClient, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI);
        final Set<TopicUriPartition> subscribedPartitionsSecure =
                subscriber.getSubscribedTopicUriPartitions(pscMetadataClientSecure, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_SECURE);

        subscribedPartitionsPlaintext.forEach(tup -> {
            if (tup.getTopicUri().getTopic().equals(TOPIC1)) {
                assertThat(tup.getTopicUri().getTopicUriAsString()).isEqualTo(TOPIC_URI1_SECURE);
            } else if (tup.getTopicUri().getTopic().equals(TOPIC2)) {
                assertThat(tup.getTopicUri().getTopicUriAsString()).isEqualTo(TOPIC_URI2);
            } else {
                throw new RuntimeException("Unexpected topic: " + tup.getTopicUri().getTopic());
            }
        });

        assertThat(subscribedPartitionsSecure).isEqualTo(subscribedPartitionsPlaintext);
    }

    @Test
    public void testNonExistingTopic() {
        final PscSubscriber subscriber =
                PscSubscriber.getTopicUriListSubscriber(
                        Collections.singletonList(NON_EXISTING_TOPIC_URI_PARTITION.getTopicUriAsString()));

        assertThatThrownBy(() -> subscriber.getSubscribedTopicUriPartitions(pscMetadataClient, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI))
                .isInstanceOf(RuntimeException.class)
                .satisfies(anyCauseMatches(UnknownTopicOrPartitionException.class));
    }

    @Test
    public void testTopicPatternSubscriber() {
        PscSubscriber subscriber =
                PscSubscriber.getTopicPatternSubscriber(Pattern.compile("pattern.*"));
        final Set<TopicUriPartition> subscribedPartitions =
                subscriber.getSubscribedTopicUriPartitions(pscMetadataClient, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI);

        final Set<TopicUriPartition> expectedSubscribedPartitions =
                new HashSet<>(
                        PscSourceTestEnv.getPartitionsForTopics(Collections.singleton(TOPIC_URI2)));

        assertThat(subscribedPartitions).isEqualTo(expectedSubscribedPartitions);
    }

    @Test
    public void testPartitionSetSubscriber() {
        List<String> topics = Arrays.asList(TOPIC_URI1, TOPIC_URI2);
        Set<TopicUriPartition> partitions =
                new HashSet<>(PscSourceTestEnv.getPartitionsForTopics(topics));
        partitions.remove(new TopicUriPartition(TOPIC_URI1, 1));

        PscSubscriber subscriber = PscSubscriber.getPartitionSetSubscriber(partitions);

        final Set<TopicUriPartition> subscribedPartitions =
                subscriber.getSubscribedTopicUriPartitions(pscMetadataClient, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI);

        assertThat(subscribedPartitions).isEqualTo(partitions);
    }

    @Test
    public void testNonExistingPartition() {
        TopicUriPartition nonExistingPartition = new TopicUriPartition(TOPIC_URI1, Integer.MAX_VALUE);
        final PscSubscriber subscriber =
                PscSubscriber.getPartitionSetSubscriber(
                        Collections.singleton(nonExistingPartition));

        assertThatThrownBy(() -> subscriber.getSubscribedTopicUriPartitions(pscMetadataClient, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(String.format("Partition '%s' does not exist on PubSub brokers", nonExistingPartition));
    }
}
