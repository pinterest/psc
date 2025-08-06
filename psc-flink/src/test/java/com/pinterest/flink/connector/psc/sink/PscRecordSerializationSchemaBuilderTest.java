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

package com.pinterest.flink.connector.psc.sink;

import com.google.common.collect.Maps;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.psc.common.PscPlugin;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.Deserializer;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.testutils.formats.DummyInitializationContext;
import org.apache.flink.util.TestLogger;


import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link PscRecordSerializationSchemaBuilder}. */
public class PscRecordSerializationSchemaBuilderTest extends TestLogger {

    private static final String DEFAULT_TOPIC = "test";

    private static Map<String, ?> configurableConfiguration;
    private static Map<String, ?> configuration;
    private static boolean isKeySerializer;

    @Before
    public void setUp() {
        configurableConfiguration = new HashMap<>();
        configuration = new HashMap<>();
        isKeySerializer = false;
    }

    @Test
    public void testDoNotAllowMultipleKeySerializer() {
        assertOnlyOneSerializerAllowed(keySerializationSetter());
    }

    @Test
    public void testDoNotAllowMultipleValueSerializer() {
        assertOnlyOneSerializerAllowed(valueSerializationSetter());
    }

    @Test
    public void testDoNotAllowMultipleTopicSelector() {
        assertThatThrownBy(
                () ->
                        PscRecordSerializationSchema.builder()
                                .setTopicUriSelector(e -> DEFAULT_TOPIC)
                                .setTopicUriString(DEFAULT_TOPIC))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                () ->
                        PscRecordSerializationSchema.builder()
                                .setTopicUriString(DEFAULT_TOPIC)
                                .setTopicUriSelector(e -> DEFAULT_TOPIC))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testExpectTopicSelector() {
        assertThatThrownBy(
                PscRecordSerializationSchema.builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        ::build)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testExpectValueSerializer() {
        assertThatThrownBy(PscRecordSerializationSchema.builder().setTopicUriString(DEFAULT_TOPIC)::build)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testSerializeRecordWithTopicSelector() {
        final TopicUriSelector<String> topicSelector =
                (e) -> {
                    if (e.equals("a")) {
                        return "topic-a";
                    }
                    return "topic-b";
                };
        final PscRecordSerializationSchemaBuilder<String> builder =
                PscRecordSerializationSchema.builder().setTopicUriSelector(topicSelector);
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final PscRecordSerializationSchema<String> schema =
                builder.setValueSerializationSchema(serializationSchema).build();
        final PscProducerMessage<byte[], byte[]> record = schema.serialize("a", null, null);
        assertThat(record.getTopicUriAsString()).isEqualTo("topic-a");
        assertThat(record.getKey()).isNull();
        assertThat(record.getValue()).isEqualTo(serializationSchema.serialize("a"));

        final PscProducerMessage<byte[], byte[]> record2 = schema.serialize("b", null, null);
        assertThat(record2.getTopicUriAsString()).isEqualTo("topic-b");
        assertThat(record2.getKey()).isNull();
        assertThat(record2.getValue()).isEqualTo(serializationSchema.serialize("b"));
    }

    @Test
    public void testSerializeRecordWithPartitioner() throws Exception {
        AtomicBoolean opened = new AtomicBoolean(false);
        final int partition = 5;
        final FlinkPscPartitioner<Object> partitioner =
                new ConstantPartitioner<>(opened, partition);
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(partitioner)
                        .build();
        final PscRecordSerializationSchema.PscSinkContext sinkContext = new TestSinkContext();
        schema.open(null, sinkContext);
        final PscProducerMessage<byte[], byte[]> record = schema.serialize("a", sinkContext, null);
        assertThat(record.getPartition()).isEqualTo(partition);
        assertThat(opened.get()).isTrue();
    }

    @Test
    public void testSerializeRecordWithHeaderProvider() throws Exception {
        final HeaderProvider<String> headerProvider =
                (ignored) ->
                        Collections.singletonMap("a", "a".getBytes(StandardCharsets.UTF_8));

        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setHeaderProvider(headerProvider)
                        .build();
        final PscProducerMessage<byte[], byte[]> record = schema.serialize("a", null, null);
        assertThat(record).isNotNull();
        assertThat(record.getHeaders()).containsExactly(Maps.immutableEntry("a", "a".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testSerializeRecordWithKey() {
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();
        final PscProducerMessage<byte[], byte[]> record = schema.serialize("a", null, null);
        assertThat(serializationSchema.serialize("a"))
                .isEqualTo(record.getKey())
                .isEqualTo(record.getValue());
    }

    @Test
    public void testPscKeySerializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = Collections.singletonMap("simpleKey", "simpleValue");
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        // Use StringSerializer as dummy Serializer, since ValueSerializer is
                        // mandatory.
                        .setPscValueSerializer(StringSerializer.class, config)
                        .setPscKeySerializer(SimpleStringSerializer.class, config)
                        .build();
        open(schema);
        assertThat(config).isEqualTo(configuration);
        assertThat(isKeySerializer).isTrue();
        assertThat(configurableConfiguration).isEmpty();
    }

    @Test
    public void tesPscValueSerializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = Collections.singletonMap("simpleKey", "simpleValue");
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        .setPscValueSerializer(SimpleStringSerializer.class, config)
                        .build();
        open(schema);
        assertThat(config).isEqualTo(configuration);
        assertThat(isKeySerializer).isFalse();
        assertThat(configurableConfiguration).isEmpty();
    }

    @Test
    public void testSerializeRecordWithPscSerializer() throws Exception {
        final Map<String, String> config = Collections.singletonMap("configKey", "configValue");
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        .setPscValueSerializer(ConfigurableStringSerializer.class, config)
                        .build();
        open(schema);
        assertThat(config).isEqualTo(configurableConfiguration);
        assertThat(configuration).isEmpty();
        final Deserializer<String> deserializer = new StringDeserializer();
        final PscProducerMessage<byte[], byte[]> record = schema.serialize("a", null, null);
        assertThat(deserializer.deserialize(record.getValue())).isEqualTo("a");
    }

    @Test
    public void testSerializeRecordWithTimestamp() {
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();
        final PscProducerMessage<byte[], byte[]> recordWithTimestamp =
                schema.serialize("a", null, 100L);
        assertThat(recordWithTimestamp.getPublishTimestamp()).isEqualTo(100L);

        final PscProducerMessage<byte[], byte[]> recordWithTimestampZero =
                schema.serialize("a", null, 0L);
        assertThat(recordWithTimestampZero.getPublishTimestamp()).isEqualTo(0L);

        // the below tests are commented out because PSC core injects the timestamp if it's null

//        final PscProducerMessage<byte[], byte[]> recordWithoutTimestamp =
//                schema.serialize("a", null, null);
//        assertNull(recordWithoutTimestamp.getPublishTimestamp());
//
//        final PscProducerMessage<byte[], byte[]> recordWithInvalidTimestamp =
//                schema.serialize("a", null, -100L);
//        assertNull(recordWithInvalidTimestamp.getPublishTimestamp());
    }

    private static void assertOnlyOneSerializerAllowed(
            List<
                            Function<
                                    PscRecordSerializationSchemaBuilder<String>,
                                    PscRecordSerializationSchemaBuilder<String>>>
                    serializers) {
        for (final Function<
                PscRecordSerializationSchemaBuilder<String>,
                PscRecordSerializationSchemaBuilder<String>>
                setter : serializers) {
            final PscRecordSerializationSchemaBuilder<String> builder =
                    PscRecordSerializationSchema.<String>builder().setTopicUriString(DEFAULT_TOPIC);
            setter.apply(builder);
            for (final Function<
                    PscRecordSerializationSchemaBuilder<String>,
                    PscRecordSerializationSchemaBuilder<String>>
                    updater : serializers) {
                assertThatThrownBy(() -> updater.apply(builder))
                        .isInstanceOf(IllegalStateException.class);
            }
        }
    }

    private static List<
                    Function<
                            PscRecordSerializationSchemaBuilder<String>,
                            PscRecordSerializationSchemaBuilder<String>>>
            valueSerializationSetter() {
        return Arrays.asList(
                (b) -> b.setPscValueSerializer(StringSerializer.class),
                (b) -> b.setValueSerializationSchema(new SimpleStringSchema()),
                (b) ->
                        b.setPscValueSerializer(
                                ConfigurableStringSerializer.class, Collections.emptyMap()));
    }

    private static List<
                    Function<
                            PscRecordSerializationSchemaBuilder<String>,
                            PscRecordSerializationSchemaBuilder<String>>>
            keySerializationSetter() {
        return Arrays.asList(
                (b) -> b.setPscKeySerializer(StringSerializer.class),
                (b) -> b.setKeySerializationSchema(new SimpleStringSchema()),
                (b) ->
                        b.setPscKeySerializer(
                                ConfigurableStringSerializer.class, Collections.emptyMap()));
    }

    /**
     * Serializer based on PSC's serialization stack. This is the special case that implements
     * {@link PscPlugin}
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class ConfigurableStringSerializer extends StringSerializer implements PscPlugin {

        @Override
        public void configure(Map<String, String> configs, boolean isKey) {
            configurableConfiguration = configs;
        }
    }

    /**
     * Serializer based on PSC's serialization stack.
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class SimpleStringSerializer extends StringSerializer {
        @Override
        public void configure(Map<String, String> configs, boolean isKey) {
            configuration = configs;
            isKeySerializer = isKey;
        }
    }

    private static class TestSinkContext
            implements PscRecordSerializationSchema.PscSinkContext {
        @Override
        public int getParallelInstanceId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelInstances() {
            return 0;
        }

        @Override
        public int[] getPartitionsForTopicUri(String topicUri) {
            return new int[0];
        }
    }

    private static class ConstantPartitioner<T> extends FlinkPscPartitioner<T> {

        private final AtomicBoolean opened;
        private final int partition;

        ConstantPartitioner(AtomicBoolean opened, int partition) {
            this.opened = opened;
            this.partition = partition;
        }

        @Override
        public void open(int parallelInstanceId, int parallelInstances) {
            opened.set(true);
        }

        @Override
        public int partition(
                T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return partition;
        }
    }

    private void open(PscRecordSerializationSchema<String> schema) throws Exception {
        schema.open(new DummyInitializationContext(), null);
    }
}
