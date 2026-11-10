package com.pinterest.psc.consumer.memq;

import com.pinterest.memq.client.consumer.MemqConsumer;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPscMemqConsumerTopicUriPartition {
    private static final String TOPIC = "topic01";

    @Test
    @SuppressWarnings("unchecked")
    void assignmentUsesTheConsumerTopicUri() throws Exception {
        MemqTopicUri topicUri = memqTopicUri();
        MemqConsumer<byte[], byte[]> memqConsumer = mock(MemqConsumer.class);
        when(memqConsumer.assignment()).thenReturn(new HashSet<>(Arrays.asList(0, 3)));
        PscMemqConsumer<byte[], byte[]> consumer = consumer(memqConsumer, topicUri);

        Set<TopicUriPartition> assignment = consumer.assignment();

        assertEquals(setOf(legacy(topicUri, 0), legacy(topicUri, 3)), assignment);
        for (TopicUriPartition partition : assignment) {
            assertSame(topicUri, partition.getTopicUri());
            assertEquals(topicUri.getTopicUriAsString(), partition.getTopicUriAsString());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void commitAsyncCallbackUsesTheConsumerTopicUri() throws Exception {
        MemqTopicUri topicUri = memqTopicUri();
        MemqConsumer<byte[], byte[]> memqConsumer = mock(MemqConsumer.class);
        doAnswer(invocation -> {
            com.pinterest.memq.client.consumer.OffsetCommitCallback callback = invocation.getArgument(1);
            Map<Integer, Long> offsets = new HashMap<>();
            offsets.put(4, 10L);
            callback.onCompletion(offsets, null);
            return null;
        }).when(memqConsumer).commitOffsetAsync(anyMap(), any(com.pinterest.memq.client.consumer.OffsetCommitCallback.class));

        PscMemqConsumer<byte[], byte[]> consumer = consumer(memqConsumer, topicUri);
        TopicUriPartition messagePartition = new TopicUriPartition(topicUri, 4);
        MessageId messageId = new MemqMessageId(messagePartition, new MemqOffset(6L, 0L).toLong(), false);
        Map<TopicUriPartition, MessageId> committed = new HashMap<>();

        consumer.commitAsync(Collections.singleton(messageId), (result, exception) -> {
            assertNull(exception);
            committed.putAll(result);
        });

        TopicUriPartition expected = legacy(topicUri, 4);
        assertEquals(1, committed.size());
        assertEquals(expected, committed.keySet().iterator().next());
        assertSame(topicUri, committed.keySet().iterator().next().getTopicUri());
        assertEquals(new MemqMessageId(expected, 10L, true), committed.get(expected));
    }

    @SuppressWarnings("unchecked")
    private static PscMemqConsumer<byte[], byte[]> consumer(MemqConsumer<byte[], byte[]> memqConsumer, MemqTopicUri topicUri) throws Exception {
        PscMemqConsumer<byte[], byte[]> consumer = new PscMemqConsumer<>();
        setField(consumer, "memqConsumer", memqConsumer);
        setField(consumer, "topicUri", topicUri);
        return consumer;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = PscMemqConsumer.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static TopicUriPartition legacy(MemqTopicUri topicUri, int partition) {
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUri.getTopicUriAsString(), partition);
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
        return topicUriPartition;
    }

    private static Set<TopicUriPartition> setOf(TopicUriPartition... partitions) {
        return new HashSet<>(Arrays.asList(partitions));
    }

    private static MemqTopicUri memqTopicUri() throws TopicUriSyntaxException {
        String topicUri = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":memq:env:aws_us-west-1::cluster:" + TOPIC;
        return MemqTopicUri.validate(TopicUri.validate(topicUri));
    }
}
