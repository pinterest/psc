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

package com.pinterest.flink.connector.psc.dynamic.source.split;

import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.psc.common.TopicUriPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A test for {@link DynamicPscSourceSplitSerializer}.
 */
public class DynamicPscSourceSplitSerializerTest {

    @Test
    public void testSerde() throws IOException {
        DynamicPscSourceSplitSerializer serializer = new DynamicPscSourceSplitSerializer();
        DynamicPscSourceSplit dynamicPscSourceSplit =
                new DynamicPscSourceSplit(
                        "test-cluster",
                        new PscTopicUriPartitionSplit(new TopicUriPartition(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + "test-topic", 3), 1));
        DynamicPscSourceSplit dynamicPscSourceSplitAfterSerde =
                serializer.deserialize(1, serializer.serialize(dynamicPscSourceSplit));
        assertEquals(dynamicPscSourceSplit, dynamicPscSourceSplitAfterSerde);
    }
}
