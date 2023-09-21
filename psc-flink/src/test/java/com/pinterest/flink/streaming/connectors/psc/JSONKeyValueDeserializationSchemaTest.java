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

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumerMessage;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import com.pinterest.flink.streaming.util.serialization.psc.JSONKeyValueDeserializationSchema;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;

/**
 * Tests for the{@link JSONKeyValueDeserializationSchema}.
 */
public class JSONKeyValueDeserializationSchemaTest {

    private static final String VALID_TOPIC_RN = TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:topic01";

    @Test
    public void testDeserializeWithoutMetadata() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode initialKey = mapper.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

        ObjectNode initialValue = mapper.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        ObjectNode deserializedValue = schema
                .deserialize(newConsumerRecord(serializedKey, serializedValue));

        Assert.assertTrue(deserializedValue.get("metadata") == null);
        Assert.assertEquals(4, deserializedValue.get("key").get("index").asInt());
        Assert.assertEquals("world", deserializedValue.get("value").get("word").asText());
    }

    @Test
    public void testDeserializeWithoutKey() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        byte[] serializedKey = null;

        ObjectNode initialValue = mapper.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        ObjectNode deserializedValue = schema
                .deserialize(newConsumerRecord(serializedKey, serializedValue));

        Assert.assertTrue(deserializedValue.get("metadata") == null);
        Assert.assertTrue(deserializedValue.get("key") == null);
        Assert.assertEquals("world", deserializedValue.get("value").get("word").asText());
    }

    private static PscConsumerMessage<byte[], byte[]> newConsumerRecord(byte[] serializedKey,
                                                                        byte[] serializedValue) throws URISyntaxException {
        return newConsumerRecord(VALID_TOPIC_RN, 0, 0L, serializedKey, serializedValue);
    }

    private static PscConsumerMessage<byte[], byte[]> newConsumerRecord(String topic,
                                                                        int partition,
                                                                        long offset,
                                                                        byte[] serializedKey,
                                                                        byte[] serializedValue) throws URISyntaxException {

        return new PscConsumerMessage<>(new MessageId(new TopicUriPartition(topic, partition), offset),
                serializedKey, serializedValue, 1L);
    }

    @Test
    public void testDeserializeWithoutValue() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode initialKey = mapper.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

        byte[] serializedValue = null;

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        ObjectNode deserializedValue = schema
                .deserialize(newConsumerRecord(serializedKey, serializedValue));

        Assert.assertTrue(deserializedValue.get("metadata") == null);
        Assert.assertEquals(4, deserializedValue.get("key").get("index").asInt());
        Assert.assertTrue(deserializedValue.get("value") == null);
    }

    @Test
    public void testDeserializeWithMetadata() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode initialKey = mapper.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = mapper.writeValueAsBytes(initialKey);

        ObjectNode initialValue = mapper.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = mapper.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(true);
        final PscConsumerMessage<byte[], byte[]> consumerRecord = newConsumerRecord(
                VALID_TOPIC_RN, 3, 4L,
                serializedKey, serializedValue);
        ObjectNode deserializedValue = schema.deserialize(consumerRecord);

        Assert.assertEquals(4, deserializedValue.get("key").get("index").asInt());
        Assert.assertEquals("world", deserializedValue.get("value").get("word").asText());
        Assert.assertEquals(VALID_TOPIC_RN, deserializedValue.get("metadata").get("topic").asText());
        Assert.assertEquals(4, deserializedValue.get("metadata").get("offset").asInt());
        Assert.assertEquals(3, deserializedValue.get("metadata").get("partition").asInt());
    }
}
