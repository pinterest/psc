/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.source.split;

import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PscTopicUriPartitionSplitSerializer}. */
public class PscTopicUriPartitionSplitSerializerTest {

    @Test
    public void testSerializer() throws IOException {
        String topic = "topic";
        String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + topic;
        Long offsetZero = 0L;
        Long normalOffset = 1L;
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUri, 1);
        List<Long> stoppingOffsets =
                Lists.newArrayList(PscTopicUriPartitionSplit.COMMITTED_OFFSET, offsetZero, normalOffset);
        PscTopicUriPartitionSplitSerializer splitSerializer = new PscTopicUriPartitionSplitSerializer();
        for (Long stoppingOffset : stoppingOffsets) {
            PscTopicUriPartitionSplit kafkaPartitionSplit =
                    new PscTopicUriPartitionSplit(topicUriPartition, 0, stoppingOffset);
            byte[] serialize = splitSerializer.serialize(kafkaPartitionSplit);
            PscTopicUriPartitionSplit deserializeSplit =
                    splitSerializer.deserialize(splitSerializer.getVersion(), serialize);
            assertThat(deserializeSplit).isEqualTo(kafkaPartitionSplit);
        }
    }
}
