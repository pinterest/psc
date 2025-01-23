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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pinterest.flink.connector.psc.util.JacksonMapperFactory;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.flink.streaming.util.serialization.psc.JSONKeyValueDeserializationSchema;
import org.apache.flink.connector.testutils.formats.DummyInitializationContext;
import org.junit.Test;

import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the{@link JSONKeyValueDeserializationSchema}.
 */
public class JSONKeyValueDeserializationSchemaTest {

    private static final String VALID_TOPIC_RN = TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:topic01";
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    @Test
    public void testDeserializeWithoutMetadata() throws Exception {
        ObjectNode initialKey = OBJECT_MAPPER.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = OBJECT_MAPPER.writeValueAsBytes(initialKey);

        ObjectNode initialValue = OBJECT_MAPPER.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = OBJECT_MAPPER.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        schema.open(new DummyInitializationContext());
        ObjectNode deserializedValue =
                schema.deserialize(newConsumerRecord(serializedKey, serializedValue));

        assertThat(deserializedValue.get("metadata")).isNull();
        assertThat(deserializedValue.get("key").get("index").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("value").get("word").asText()).isEqualTo("world");
    }

    @Test
    public void testDeserializeWithoutKey() throws Exception {
        byte[] serializedKey = null;

        ObjectNode initialValue = OBJECT_MAPPER.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = OBJECT_MAPPER.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        schema.open(new DummyInitializationContext());
        ObjectNode deserializedValue = schema
                .deserialize(newConsumerRecord(serializedKey, serializedValue));

        assertThat(deserializedValue.get("metadata")).isNull();
        assertThat(deserializedValue.get("key")).isNull();
        assertThat(deserializedValue.get("value").get("word").asText()).isEqualTo("world");
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
        ObjectNode initialKey = OBJECT_MAPPER.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = OBJECT_MAPPER.writeValueAsBytes(initialKey);

        byte[] serializedValue = null;

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(false);
        schema.open(new DummyInitializationContext());
        ObjectNode deserializedValue =
                schema.deserialize(newConsumerRecord(serializedKey, serializedValue));

        assertThat(deserializedValue.get("metadata")).isNull();
        assertThat(deserializedValue.get("key").get("index").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("value")).isNull();
    }

    @Test
    public void testDeserializeWithMetadata() throws Exception {
        ObjectNode initialKey = OBJECT_MAPPER.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = OBJECT_MAPPER.writeValueAsBytes(initialKey);

        ObjectNode initialValue = OBJECT_MAPPER.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = OBJECT_MAPPER.writeValueAsBytes(initialValue);

        JSONKeyValueDeserializationSchema schema = new JSONKeyValueDeserializationSchema(true);
        schema.open(new DummyInitializationContext());
        final PscConsumerMessage<byte[], byte[]> consumerRecord = newConsumerRecord(
                VALID_TOPIC_RN, 3, 4L,
                serializedKey, serializedValue);
        ObjectNode deserializedValue = schema.deserialize(consumerRecord);

        assertThat(deserializedValue.get("key").get("index").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("value").get("word").asText()).isEqualTo("world");
        assertThat(deserializedValue.get("metadata").get("topic").asText()).isEqualTo(VALID_TOPIC_RN);
        assertThat(deserializedValue.get("metadata").get("offset").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("metadata").get("partition").asInt()).isEqualTo(3);
    }
}
