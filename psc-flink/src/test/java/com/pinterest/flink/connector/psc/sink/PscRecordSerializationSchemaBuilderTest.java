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

import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.psc.common.PscPlugin;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.Deserializer;
import com.pinterest.psc.serde.StringDeserializer;
import com.pinterest.psc.serde.StringSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

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
        assertThrows(
                IllegalStateException.class,
                () ->
                        PscRecordSerializationSchema.builder()
                                .setTopicUriSelector(e -> DEFAULT_TOPIC)
                                .setTopicUriString(DEFAULT_TOPIC));
        assertThrows(
                IllegalStateException.class,
                () ->
                        PscRecordSerializationSchema.builder()
                                .setTopicUriString(DEFAULT_TOPIC)
                                .setTopicUriSelector(e -> DEFAULT_TOPIC));
    }

    @Test
    public void testExpectTopicSelector() {
        assertThrows(
                IllegalStateException.class,
                PscRecordSerializationSchema.builder()
                                .setValueSerializationSchema(new SimpleStringSchema())
                        ::build);
    }

    @Test
    public void testExpectValueSerializer() {
        assertThrows(
                IllegalStateException.class,
                PscRecordSerializationSchema.builder().setTopicUriString(DEFAULT_TOPIC)::build);
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
        assertEquals("topic-a", record.getTopicUriAsString());
        assertNull(record.getKey());
        assertArrayEquals(serializationSchema.serialize("a"), record.getValue());

        final PscProducerMessage<byte[], byte[]> record2 = schema.serialize("b", null, null);
        assertEquals("topic-b", record2.getTopicUriAsString());
        assertNull(record2.getKey());
        assertArrayEquals(serializationSchema.serialize("b"), record2.getValue());
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
        assertEquals(partition, record.getPartition());
        assertTrue(opened.get());
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
        assertArrayEquals(record.getKey(), serializationSchema.serialize("a"));
        assertArrayEquals(record.getValue(), serializationSchema.serialize("a"));
    }

    @Test
    public void testKafkaKeySerializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = ImmutableMap.of("simpleKey", "simpleValue");
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        // Use StringSerializer as dummy Serializer, since ValueSerializer is
                        // mandatory.
                        .setPscValueSerializer(StringSerializer.class, config)
                        .setPscKeySerializer(SimpleStringSerializer.class, config)
                        .build();
        open(schema);
        assertEquals(configuration, config);
        assertTrue(isKeySerializer);
        assertTrue(configurableConfiguration.isEmpty());
    }

    @Test
    public void testKafkaValueSerializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = ImmutableMap.of("simpleKey", "simpleValue");
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        .setPscValueSerializer(SimpleStringSerializer.class, config)
                        .build();
        open(schema);
        assertEquals(configuration, config);
        assertFalse(isKeySerializer);
        assertTrue(configurableConfiguration.isEmpty());
    }

    @Test
    public void testSerializeRecordWithKafkaSerializer() throws Exception {
        final Map<String, String> config = ImmutableMap.of("configKey", "configValue");
        final PscRecordSerializationSchema<String> schema =
                PscRecordSerializationSchema.builder()
                        .setTopicUriString(DEFAULT_TOPIC)
                        .setPscValueSerializer(ConfigurableStringSerializer.class, config)
                        .build();
        open(schema);
        assertEquals(configurableConfiguration, config);
        assertTrue(configuration.isEmpty());
        final Deserializer<String> deserializer = new StringDeserializer();
        final PscProducerMessage<byte[], byte[]> record = schema.serialize("a", null, null);
        assertEquals("a", deserializer.deserialize(record.getValue()));
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
        assertEquals(100L, (long) recordWithTimestamp.getPublishTimestamp());

        final PscProducerMessage<byte[], byte[]> recordWithTimestampZero =
                schema.serialize("a", null, 0L);
        assertEquals(0L, (long) recordWithTimestampZero.getPublishTimestamp());

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
                assertThrows(IllegalStateException.class, () -> updater.apply(builder));
            }
        }
    }

    private static List<
                    Function<
                            PscRecordSerializationSchemaBuilder<String>,
                            PscRecordSerializationSchemaBuilder<String>>>
            valueSerializationSetter() {
        return ImmutableList.of(
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
        return ImmutableList.of(
                (b) -> b.setPscKeySerializer(StringSerializer.class),
                (b) -> b.setKeySerializationSchema(new SimpleStringSchema()),
                (b) ->
                        b.setPscKeySerializer(
                                ConfigurableStringSerializer.class, Collections.emptyMap()));
    }

    /**
     * Serializer based on Kafka's serialization stack. This is the special case that implements
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
     * Serializer based on Kafka's serialization stack.
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
        schema.open(
                new SerializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return null;
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return new UserCodeClassLoader() {
                            @Override
                            public ClassLoader asClassLoader() {
                                return PscRecordSerializationSchemaBuilderTest.class
                                        .getClassLoader();
                            }

                            @Override
                            public void registerReleaseHookIfAbsent(
                                    String releaseHookName, Runnable releaseHook) {}
                        };
                    }
                },
                null);
    }
}
