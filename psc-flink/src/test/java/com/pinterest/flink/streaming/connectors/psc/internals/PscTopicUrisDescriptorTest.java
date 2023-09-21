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

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.psc.common.TopicUri;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Tests for the {@link PscTopicUrisDescriptor}.
 */
@RunWith(Parameterized.class)
public class PscTopicUrisDescriptorTest {
    private static final String TOPIC_URI_PREFIX =
            String.format("plaintext:%s%s:kafka:env:cloud_region::cluster:topic", TopicUri.SEPARATOR, TopicUri.STANDARD);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {
                    TOPIC_URI_PREFIX + "topic1",
                    null,
                    Arrays.asList(TOPIC_URI_PREFIX + "topic1", TOPIC_URI_PREFIX + "topic2", TOPIC_URI_PREFIX + "topic3"),
                    true
                },
                {
                    TOPIC_URI_PREFIX + "topic1",
                    null,
                    Arrays.asList(TOPIC_URI_PREFIX + "topic2", TOPIC_URI_PREFIX + "topic3"),
                    false
                },
                {
                    TOPIC_URI_PREFIX + "topic1",
                    Pattern.compile(TOPIC_URI_PREFIX + "topic[0-9]"),
                    null,
                    true
                },
                {
                    TOPIC_URI_PREFIX + "topicx",
                    Pattern.compile(TOPIC_URI_PREFIX + "topic[0-9]"),
                    null,
                    false
                }
        });
    }

    private String topic;
    private Pattern topicPattern;
    private List<String> fixedTopics;
    boolean expected;

    public PscTopicUrisDescriptorTest(String topic, Pattern topicPattern, List<String> fixedTopics, boolean expected) {
        this.topic = topic;
        this.topicPattern = topicPattern;
        this.fixedTopics = fixedTopics;
        this.expected = expected;
    }

    @Test
    public void testIsMatchingTopic() {
        PscTopicUrisDescriptor topicsDescriptor = new PscTopicUrisDescriptor(fixedTopics, topicPattern);

        Assert.assertEquals(expected, topicsDescriptor.isMatchingTopicUri(topic));
    }
}
