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

package com.pinterest.flink.connector.psc.source.reader.deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pinterest.flink.connector.psc.util.JacksonMapperFactory;
import com.pinterest.flink.streaming.util.serialization.psc.JSONKeyValueDeserializationSchema;
import com.pinterest.psc.common.PscPlugin;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.flink.connector.testutils.formats.DummyInitializationContext;
import org.apache.flink.connector.testutils.source.deserialization.TestingDeserializationContext;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for PscRecordDeserializationSchema. */
public class PscRecordDeserializationSchemaTest {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();
    private static Map<String, String> configurableConfiguration;
    private static Map<String, String> configuration;
    private static boolean isKeyDeserializer;

    @Before
    public void setUp() {
        configurableConfiguration = new HashMap<>(1);
        configuration = new HashMap<>(1);
        isKeyDeserializer = false;
    }

    @Test
    public void testPscDeserializationSchemaWrapper() throws Exception {
        final PscConsumerMessage<byte[], byte[]> consumerRecord = getPscConsumerMessage();
        PscRecordDeserializationSchema<ObjectNode> schema =
                PscRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true));
        schema.open(new DummyInitializationContext());
        SimpleCollector<ObjectNode> collector = new SimpleCollector<>();
        schema.deserialize(consumerRecord, collector);

        assertThat(collector.list).hasSize(1);
        ObjectNode deserializedValue = collector.list.get(0);

        assertThat(deserializedValue.get("key").get("index").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("value").get("word").asText()).isEqualTo("world");
        assertThat(deserializedValue.get("metadata").get("topic").asText()).isEqualTo("topic#1");
        assertThat(deserializedValue.get("metadata").get("offset").asInt()).isEqualTo(4);
        assertThat(deserializedValue.get("metadata").get("partition").asInt()).isEqualTo(3);
    }

    @Test
    public void testPscValueDeserializationSchemaWrapper() throws Exception {
        final PscConsumerMessage<byte[], byte[]> consumerRecord = getPscConsumerMessage();
        PscRecordDeserializationSchema<
                org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node
                        .ObjectNode>
                schema =
                PscRecordDeserializationSchema.valueOnly(
                        new JsonDeserializationSchema<>(
                                org.apache.flink.shaded.jackson2.com.fasterxml.jackson
                                        .databind.node.ObjectNode.class));
        schema.open(new DummyInitializationContext());
        SimpleCollector<
                org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node
                        .ObjectNode>
                collector = new SimpleCollector<>();
        schema.deserialize(consumerRecord, collector);

        assertThat(collector.list).hasSize(1);
        org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
                deserializedValue = collector.list.get(0);
        assertThat(deserializedValue.get("word").asText()).isEqualTo("world");
        assertThat(deserializedValue.get("key")).isNull();
        assertThat(deserializedValue.get("metadata")).isNull();
    }

    @Test
    public void testPscValueDeserializerWrapper() throws Exception {
        final String topic = "Topic";
        byte[] value = new StringSerializer().serialize(topic, "world");
        final PscConsumerMessage<byte[], byte[]> consumerRecord =
                new PscConsumerMessage<>(topic, 0, 0L, null, value);
        PscRecordDeserializationSchema<String> schema =
                PscRecordDeserializationSchema.valueOnly(StringDeserializer.class);
        schema.open(new TestingDeserializationContext());

        SimpleCollector<String> collector = new SimpleCollector<>();
        schema.deserialize(consumerRecord, collector);

        assertThat(collector.list).hasSize(1);
        assertThat(collector.list.get(0)).isEqualTo("world");
    }

    @Test
    public void testPscValueDeserializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = Collections.singletonMap("simpleKey", "simpleValue");
        PscRecordDeserializationSchema<String> schema =
                PscRecordDeserializationSchema.valueOnly(SimpleStringSerializer.class, config);
        schema.open(new TestingDeserializationContext());
        assertThat(config).isEqualTo(configuration);
        assertThat(isKeyDeserializer).isFalse();
        assertThat(configurableConfiguration).isEmpty();
    }

    @Test
    public void testPscValueDeserializerWrapperWithConfigurable() throws Exception {
        final Map<String, String> config = Collections.singletonMap("configKey", "configValue");
        PscRecordDeserializationSchema<String> schema =
                PscRecordDeserializationSchema.valueOnly(
                        ConfigurableStringSerializer.class, config);
        schema.open(new TestingDeserializationContext());
        assertThat(config).isEqualTo(configurableConfiguration);
        assertThat(isKeyDeserializer).isFalse();
        assertThat(configuration).isEmpty();
    }

    private PscConsumerMessage<byte[], byte[]> getPscConsumerMessage() throws JsonProcessingException {
        ObjectNode initialKey = OBJECT_MAPPER.createObjectNode();
        initialKey.put("index", 4);
        byte[] serializedKey = OBJECT_MAPPER.writeValueAsBytes(initialKey);

        ObjectNode initialValue = OBJECT_MAPPER.createObjectNode();
        initialValue.put("word", "world");
        byte[] serializedValue = OBJECT_MAPPER.writeValueAsBytes(initialValue);

        return new PscConsumerMessage<>("topic#1", 3, 4L, serializedKey, serializedValue);
    }

    private static class SimpleCollector<T> implements Collector<T> {

        private final List<T> list = new ArrayList<>();

        @Override
        public void collect(T record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    /**
     * Serializer based on PSC's serialization stack. This is the special case that implements
     * {@link PscPlugin}
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class ConfigurableStringSerializer extends StringDeserializer
            implements PscPlugin {
        @Override
        public void configure(PscConfiguration pscConfig, boolean isKey) {
            pscConfig.getKeys().forEachRemaining(key -> configurableConfiguration.put(key, pscConfig.getString(key)));
        }
    }

    /**
     * Serializer based on PSC's serialization stack.
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class SimpleStringSerializer extends StringDeserializer {
        @Override
        public void configure(PscConfiguration pscConfig, boolean isKey) {
            pscConfig.getKeys().forEachRemaining(key -> configuration.put(key, pscConfig.getString(key)));
            isKeyDeserializer = isKey;
        }
    }
}
