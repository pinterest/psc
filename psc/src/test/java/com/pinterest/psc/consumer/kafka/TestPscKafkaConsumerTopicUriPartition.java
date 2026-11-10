package com.pinterest.psc.consumer.kafka;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscBackendClient;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaMessageId;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.consumer.ConsumerRebalanceListener;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPscKafkaConsumerTopicUriPartition {
    private static final String TOPIC = "topic01";

    @Test
    @SuppressWarnings("unchecked")
    void assignmentAndGetPartitionsReuseTheRegisteredTopicUri() throws Exception {
        KafkaTopicUri topicUri = kafkaTopicUri();
        Consumer<byte[], byte[]> kafkaConsumer = mock(Consumer.class);
        PscKafkaConsumer<byte[], byte[]> consumer = consumer(kafkaConsumer, false);
        consumer.subscribe(Collections.singleton(topicUri));
        when(kafkaConsumer.assignment()).thenReturn(new HashSet<>(Arrays.asList(
                new TopicPartition(TOPIC, 0),
                new TopicPartition(TOPIC, 4)
        )));

        Set<TopicUriPartition> assignment = consumer.assignment();

        assertEquals(setOf(legacy(topicUri, 0), legacy(topicUri, 4)), assignment);
        for (TopicUriPartition partition : assignment) {
            assertSame(topicUri, partition.getTopicUri());
        }

        when(kafkaConsumer.partitionsFor(TOPIC)).thenReturn(Arrays.asList(
                new PartitionInfo(TOPIC, 0, (Node) null, new Node[0], new Node[0]),
                new PartitionInfo(TOPIC, 1, (Node) null, new Node[0], new Node[0])
        ));
        assertEquals(setOf(legacy(topicUri, 0), legacy(topicUri, 1)), consumer.getPartitions(topicUri));

        when(kafkaConsumer.partitionsFor(TOPIC)).thenReturn(null);
        assertNull(consumer.getPartitions(topicUri));
    }

    @Test
    @SuppressWarnings("unchecked")
    void assignReturnsThePartitionsTheCallerSupplied() throws Exception {
        KafkaTopicUri topicUri = kafkaTopicUri();
        PscKafkaConsumer<byte[], byte[]> consumer = consumer(mock(Consumer.class), false);
        TopicUriPartition first = new TopicUriPartition(topicUri, 1);
        TopicUriPartition second = new TopicUriPartition(topicUri.getTopicUriAsString(), 2, topicUri);

        consumer.assign(new HashSet<>(Arrays.asList(first, second)));

        assertEquals(setOf(first, second), consumer.assignment());
        assertTrue(consumer.assignment().contains(first));
        assertTrue(consumer.assignment().contains(second));
        assertSame(topicUri, first.getTopicUri());
        assertSame(topicUri, second.getTopicUri());
    }

    @Test
    @SuppressWarnings("unchecked")
    void rebalanceCallbacksBuildPartitionsFromTheRegisteredTopicUri() throws Exception {
        KafkaTopicUri topicUri = kafkaTopicUri();
        Consumer<byte[], byte[]> kafkaConsumer = mock(Consumer.class);
        List<TopicUriPartition> revoked = new ArrayList<>();
        List<TopicUriPartition> assigned = new ArrayList<>();
        doAnswer(invocation -> {
            org.apache.kafka.clients.consumer.ConsumerRebalanceListener listener = invocation.getArgument(1);
            listener.onPartitionsRevoked(Collections.singleton(new TopicPartition(TOPIC, 7)));
            listener.onPartitionsAssigned(Collections.singleton(new TopicPartition(TOPIC, 8)));
            return null;
        }).when(kafkaConsumer).subscribe(anyCollection(), any(org.apache.kafka.clients.consumer.ConsumerRebalanceListener.class));

        PscKafkaConsumer<byte[], byte[]> consumer = consumer(kafkaConsumer, false);
        consumer.subscribe(Collections.singleton(topicUri), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicUriPartition> partitions) {
                revoked.addAll(partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicUriPartition> partitions) {
                assigned.addAll(partitions);
            }
        });

        assertEquals(Collections.singletonList(legacy(topicUri, 7)), revoked);
        assertEquals(Collections.singletonList(legacy(topicUri, 8)), assigned);
        assertSame(topicUri, revoked.get(0).getTopicUri());
        assertSame(topicUri, assigned.get(0).getTopicUri());
    }

    @Test
    @SuppressWarnings("unchecked")
    void committedReturnsTheCallerPartitionRatherThanParsingTheKafkaTopicName() throws Exception {
        KafkaTopicUri topicUri = kafkaTopicUri();
        Consumer<byte[], byte[]> kafkaConsumer = mock(Consumer.class);
        PscKafkaConsumer<byte[], byte[]> consumer = consumer(kafkaConsumer, false);
        TopicUriPartition callerPartition = new TopicUriPartition(topicUri.getTopicUriAsString(), 2, topicUri);
        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        committed.put(new TopicPartition(TOPIC, 2), new OffsetAndMetadata(99L));
        when(kafkaConsumer.committed(anySet())).thenReturn(committed);

        Collection<MessageId> messageIds = consumer.committed(Collections.singleton(callerPartition));

        assertEquals(1, messageIds.size());
        MessageId messageId = messageIds.iterator().next();
        assertSame(callerPartition, messageId.getTopicUriPartition());
        assertEquals(99L, messageId.getOffset());
        assertEquals(new KafkaMessageId(callerPartition, 99L), messageId);
        TopicUriPartition parsedFromKafkaTopicName = new TopicUriPartition(TOPIC, 2);
        assertNull(parsedFromKafkaTopicName.getTopicUri());
        assertNotEquals(parsedFromKafkaTopicName, messageId.getTopicUriPartition());
    }

    @Test
    @SuppressWarnings("unchecked")
    void commitCallbacksMatchTheLegacyFinalizedPartition() throws Exception {
        for (boolean autoResolutionEnabled : new boolean[] {false, true}) {
            KafkaTopicUri topicUri = kafkaTopicUri();
            Consumer<byte[], byte[]> kafkaConsumer = mock(Consumer.class);
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(TOPIC, 1), new OffsetAndMetadata(42L));
            doAnswer(invocation -> {
                org.apache.kafka.clients.consumer.OffsetCommitCallback callback = invocation.getArgument(0);
                callback.onComplete(offsets, null);
                return null;
            }).when(kafkaConsumer).commitAsync(any(org.apache.kafka.clients.consumer.OffsetCommitCallback.class));
            doAnswer(invocation -> {
                org.apache.kafka.clients.consumer.OffsetCommitCallback callback = invocation.getArgument(1);
                callback.onComplete(offsets, null);
                return null;
            }).when(kafkaConsumer).commitAsync(anyMap(), any(org.apache.kafka.clients.consumer.OffsetCommitCallback.class));

            PscKafkaConsumer<byte[], byte[]> consumer = consumer(kafkaConsumer, autoResolutionEnabled);
            consumer.subscribe(Collections.singleton(topicUri));
            Map<TopicUriPartition, MessageId> committed = new HashMap<>();
            consumer.commitAsync((result, exception) -> {
                assertNull(exception);
                committed.putAll(result);
            });

            TopicUriPartition expected = legacy(topicUri, 1);
            assertEquals(Collections.singletonMap(expected, new KafkaMessageId(expected, 42L)), committed);
            assertSame(topicUri, committed.keySet().iterator().next().getTopicUri());

            TopicUriPartition messagePartition = new TopicUriPartition(topicUri, 1);
            Map<TopicUriPartition, MessageId> committedFromMessageIds = new HashMap<>();
            consumer.commitAsync(
                    Collections.singleton(new KafkaMessageId(messagePartition, 5L)),
                    (result, exception) -> {
                        assertNull(exception);
                        committedFromMessageIds.putAll(result);
                    }
            );
            assertEquals(committed, committedFromMessageIds);
        }
    }

    @SuppressWarnings("unchecked")
    private static PscKafkaConsumer<byte[], byte[]> consumer(Consumer<byte[], byte[]> kafkaConsumer, boolean autoResolutionEnabled) throws Exception {
        PscKafkaConsumer<byte[], byte[]> consumer = new PscKafkaConsumer<>();
        setField(PscKafkaConsumer.class, consumer, "kafkaConsumer", kafkaConsumer);
        setField(PscBackendClient.class, consumer, "autoResolutionEnabled", autoResolutionEnabled);
        return consumer;
    }

    private static void setField(Class<?> type, Object target, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static TopicUriPartition legacy(KafkaTopicUri topicUri, int partition) {
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUri.getTopicUriAsString(), partition);
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
        return topicUriPartition;
    }

    private static Set<TopicUriPartition> setOf(TopicUriPartition... partitions) {
        return new HashSet<>(Arrays.asList(partitions));
    }

    private static KafkaTopicUri kafkaTopicUri() throws TopicUriSyntaxException {
        String topicUri = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::cluster:" + TOPIC;
        return KafkaTopicUri.validate(TopicUri.validate(topicUri));
    }
}
