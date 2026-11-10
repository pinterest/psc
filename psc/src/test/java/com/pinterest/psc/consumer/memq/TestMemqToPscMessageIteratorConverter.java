package com.pinterest.psc.consumer.memq;

import com.pinterest.memq.commons.MemqLogMessage;
import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.CloseableIterator;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestMemqToPscMessageIteratorConverter {
    private static final String TOPIC = "topic01";

    @Test
    @SuppressWarnings("unchecked")
    void eachRecordUsesTheRegisteredTopicUriInsteadOfParsingAgain() throws Exception {
        MemqTopicUri topicUri = memqTopicUri();
        MemqLogMessage<byte[], byte[]> first = record(1, 20L, 3L, 500L, new byte[] {1}, new byte[] {2, 3}, false, null);
        MemqLogMessage<byte[], byte[]> second = record(4, 7L, 0L, 800L, null, new byte[] {9}, true, Collections.singletonMap("h", new byte[] {6}));

        MemqToPscMessageIteratorConverter<byte[], byte[]> converter =
                new MemqToPscMessageIteratorConverter<>(TOPIC, iterator(Arrays.asList(first, second)), topicMap(topicUri), null, Collections.emptyMap());

        for (MemqLogMessage<byte[], byte[]> record : Arrays.asList(first, second)) {
            assertTrue(converter.hasNext());
            PscConsumerMessage<byte[], byte[]> message = converter.getNextBackendMessage();
            TopicUriPartition expected = legacyPartition(topicUri, record.getNotificationPartitionId());
            MemqOffset offset = new MemqOffset(record.getNotificationPartitionOffset(), record.getMessageOffsetInBatch());

            assertEquals(expected, message.getMessageId().getTopicUriPartition());
            assertSame(topicUri, message.getMessageId().getTopicUriPartition().getTopicUri());
            assertEquals(topicUri.getTopicUriAsString(), message.getMessageId().getTopicUriPartition().getTopicUriAsString());
            assertEquals(offset.toLong(), message.getMessageId().getOffset());
            assertEquals(record.getWriteTimestamp(), message.getMessageId().getTimestamp());
            assertEquals(record.getKey() == null ? -1 : record.getKey().length, message.getMessageId().getSerializedKeySizeBytes());
            assertEquals(record.getValue().length, message.getMessageId().getSerializedValueSizeBytes());
            assertArrayEquals(record.getKey(), message.getKey());
            assertArrayEquals(record.getValue(), message.getValue());
            assertEquals(record.isEndOfBatch(), ((MemqMessageId) message.getMessageId()).isEndOfBatch());
            if (record.getHeaders() != null) {
                assertArrayEquals(record.getHeaders().get("h"), message.getHeader("h"));
            }
        }
        assertFalse(converter.hasNext());
    }

    @SuppressWarnings("unchecked")
    private static MemqLogMessage<byte[], byte[]> record(
            int partition,
            long batchOffset,
            long messageOffset,
            long timestamp,
            byte[] key,
            byte[] value,
            boolean endOfBatch,
            Map<String, byte[]> headers
    ) {
        MemqLogMessage<byte[], byte[]> record = mock(MemqLogMessage.class);
        when(record.getNotificationPartitionId()).thenReturn(partition);
        when(record.getNotificationPartitionOffset()).thenReturn(batchOffset);
        when(record.getMessageOffsetInBatch()).thenReturn(messageOffset);
        when(record.getWriteTimestamp()).thenReturn(timestamp);
        when(record.getKey()).thenReturn(key);
        when(record.getValue()).thenReturn(value);
        when(record.isEndOfBatch()).thenReturn(endOfBatch);
        when(record.getHeaders()).thenReturn(headers);
        return record;
    }

    private static CloseableIterator<MemqLogMessage<byte[], byte[]>> iterator(List<MemqLogMessage<byte[], byte[]>> records) {
        Iterator<MemqLogMessage<byte[], byte[]>> delegate = records.iterator();
        return new CloseableIterator<MemqLogMessage<byte[], byte[]>>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public MemqLogMessage<byte[], byte[]> next() {
                return delegate.next();
            }

            @Override
            public void close() {
            }
        };
    }

    private static Map<String, TopicUri> topicMap(MemqTopicUri topicUri) {
        Map<String, TopicUri> topicUris = new HashMap<>();
        topicUris.put(TOPIC, topicUri);
        return topicUris;
    }

    private static TopicUriPartition legacyPartition(MemqTopicUri topicUri, int partition) {
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUri.getTopicUriAsString(), partition);
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
        return topicUriPartition;
    }

    private static MemqTopicUri memqTopicUri() throws TopicUriSyntaxException {
        String topicUri = "secure:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":memq:env:aws_us-west-1::cluster:" + TOPIC;
        return MemqTopicUri.validate(TopicUri.validate(topicUri));
    }
}
