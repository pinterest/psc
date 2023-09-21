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

package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.psc.common.TopicUri;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link PscTopicUriPartition}.
 */
public class PscTopicUriPartitionTest {

    @Test
    public void validateUid() {
        Field uidField;
        try {
            uidField = PscTopicUriPartition.class.getDeclaredField("serialVersionUID");
            uidField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            fail("serialVersionUID is not defined");
            return;
        }

        assertTrue(Modifier.isStatic(uidField.getModifiers()));
        assertTrue(Modifier.isFinal(uidField.getModifiers()));
        assertTrue(Modifier.isPrivate(uidField.getModifiers()));

        assertEquals(long.class, uidField.getType());

        // the UID has to be constant to make sure old checkpoints/savepoints can be read
        try {
            assertEquals(722083576322742325L, uidField.getLong(null));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void validateSerDe() {
        String topicRnStr = TopicUri.STANDARD + ":service:env:cloud_region::cluster:topic";

        String topicUriStr1 = TopicUri.DEFAULT_PROTOCOL + ":" + TopicUri.SEPARATOR + topicRnStr;
        assertEquals(topicUriStr1, new PscTopicUriPartition(topicUriStr1, 0).getTopicUriStr());

        String topicUriStr2 = "secure:" + TopicUri.SEPARATOR + topicRnStr;
        assertEquals(topicUriStr2, new PscTopicUriPartition(topicUriStr2, 0).getTopicUriStr());

        assertEquals(
                TopicUri.DEFAULT_PROTOCOL + ":" + TopicUri.SEPARATOR + topicRnStr,
                new PscTopicUriPartition(topicRnStr, 0).getTopicUriStr()
        );
    }
}
