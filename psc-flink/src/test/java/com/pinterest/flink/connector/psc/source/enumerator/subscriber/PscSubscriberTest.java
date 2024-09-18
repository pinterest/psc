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
import com.pinterest.psc.common.TopicUriPartition;
import org.apache.flink.util.ExceptionUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link PscSubscriber}. */
public class PscSubscriberTest {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "pattern-topic";
    private static final TopicUriPartition NON_EXISTING_TOPIC = new TopicUriPartition("removed", 0);
    private static AdminClient adminClient;

    @BeforeClass
    public static void setup() throws Throwable {
        PscSourceTestEnv.setup();
        PscSourceTestEnv.createTestTopic(TOPIC1);
        PscSourceTestEnv.createTestTopic(TOPIC2);
        adminClient = PscSourceTestEnv.getAdminClient();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        adminClient.close();
        PscSourceTestEnv.tearDown();
    }

    @Test
    public void testTopicListSubscriber() {
        List<String> topics = Arrays.asList(TOPIC1, TOPIC2);
        PscSubscriber subscriber =
                PscSubscriber.getTopicUriListSubscriber(Arrays.asList(TOPIC1, TOPIC2));
        final Set<TopicUriPartition> subscribedPartitions =
                subscriber.getSubscribedTopicUriPartitions(adminClient);

        final Set<TopicUriPartition> expectedSubscribedPartitions =
                new HashSet<>(PscSourceTestEnv.getPartitionsForTopics(topics));

        assertEquals(expectedSubscribedPartitions, subscribedPartitions);
    }

    @Test
    public void testNonExistingTopic() {
        final PscSubscriber subscriber =
                PscSubscriber.getTopicUriListSubscriber(
                        Collections.singletonList(NON_EXISTING_TOPIC.getTopicUriAsString()));

        Throwable t =
                assertThrows(
                        RuntimeException.class,
                        () -> subscriber.getSubscribedTopicUriPartitions(adminClient));

        assertTrue(
                "Exception should be caused by UnknownTopicOrPartitionException",
                ExceptionUtils.findThrowable(t, UnknownTopicOrPartitionException.class)
                        .isPresent());
    }

    @Test
    public void testTopicPatternSubscriber() {
        PscSubscriber subscriber =
                PscSubscriber.getTopicPatternSubscriber(Pattern.compile("pattern.*"));
        final Set<TopicUriPartition> subscribedPartitions =
                subscriber.getSubscribedTopicUriPartitions(adminClient);

        final Set<TopicUriPartition> expectedSubscribedPartitions =
                new HashSet<>(
                        PscSourceTestEnv.getPartitionsForTopics(Collections.singleton(TOPIC2)));

        assertEquals(expectedSubscribedPartitions, subscribedPartitions);
    }

    @Test
    public void testPartitionSetSubscriber() {
        List<String> topics = Arrays.asList(TOPIC1, TOPIC2);
        Set<TopicUriPartition> partitions =
                new HashSet<>(PscSourceTestEnv.getPartitionsForTopics(topics));
        partitions.remove(new TopicUriPartition(TOPIC1, 1));

        PscSubscriber subscriber = PscSubscriber.getPartitionSetSubscriber(partitions);

        final Set<TopicUriPartition> subscribedPartitions =
                subscriber.getSubscribedTopicUriPartitions(adminClient);

        assertEquals(partitions, subscribedPartitions);
    }

    @Test
    public void testNonExistingPartition() {
        TopicUriPartition nonExistingPartition = new TopicUriPartition(TOPIC1, Integer.MAX_VALUE);
        final PscSubscriber subscriber =
                PscSubscriber.getPartitionSetSubscriber(
                        Collections.singleton(nonExistingPartition));

        Throwable t =
                assertThrows(
                        RuntimeException.class,
                        () -> subscriber.getSubscribedTopicUriPartitions(adminClient));

        assertEquals(
                String.format(
                        "Partition '%s' does not exist on Kafka brokers", nonExistingPartition),
                t.getMessage());
    }
}
