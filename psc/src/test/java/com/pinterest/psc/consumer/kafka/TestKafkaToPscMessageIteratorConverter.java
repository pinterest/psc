package com.pinterest.psc.consumer.kafka;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.common.kafka.KafkaMessageId;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestKafkaToPscMessageIteratorConverter {
    private static final String TOPIC = "topic01";

    @Test
    void eachRecordUsesTheRegisteredTopicUriInsteadOfParsingAgain() throws Exception {
        KafkaTopicUri topicUri = kafkaTopicUri();
        byte[] key = new byte[] {1, 2};
        byte[] value = new byte[] {3, 4, 5, 6};
        byte[] header = new byte[] {7};
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                record(0, 10L, 100L, 2, 4, key, value, header),
                record(0, 11L, 101L, 2, 4, key, value, header),
                record(3, 4L, 200L, -1, 4, null, value, header)
        );

        KafkaToPscMessageIteratorConverter<byte[], byte[]> converter = converter(records, topicUri);

        assertEquals(
                new java.util.HashSet<>(Arrays.asList(legacyPartition(topicUri, 0), legacyPartition(topicUri, 3))),
                converter.getTopicUriPartitions()
        );
        for (TopicUriPartition partition : converter.getTopicUriPartitions()) {
            assertSame(topicUri, partition.getTopicUri());
        }

        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertTrue(converter.hasNext());
            PscConsumerMessage<byte[], byte[]> message = converter.getNextBackendMessage();
            TopicUriPartition expected = legacyPartition(topicUri, record.partition());

            assertEquals(expected, message.getMessageId().getTopicUriPartition());
            assertSame(topicUri, message.getMessageId().getTopicUriPartition().getTopicUri());
            assertEquals(topicUri.getTopicUriAsString(), message.getMessageId().getTopicUriPartition().getTopicUriAsString());
            assertEquals(record.offset(), message.getMessageId().getOffset());
            assertEquals(record.timestamp(), message.getMessageId().getTimestamp());
            assertEquals(record.serializedKeySize(), message.getMessageId().getSerializedKeySizeBytes());
            assertEquals(record.serializedValueSize(), message.getMessageId().getSerializedValueSizeBytes());
            assertArrayEquals(record.key(), message.getKey());
            assertArrayEquals(record.value(), message.getValue());
            assertEquals(record.timestamp(), message.getPublishTimestamp());
            assertArrayEquals(header, message.getHeader("h"));
            assertEquals(
                    new KafkaMessageId(expected, record.offset(), record.timestamp(), record.serializedKeySize(), record.serializedValueSize()),
                    message.getMessageId()
            );
        }
        assertFalse(converter.hasNext());
    }

    @Test
    void iteratorForOnePartitionOnlyExposesThatPartition() throws Exception {
        KafkaTopicUri topicUri = kafkaTopicUri();
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                record(1, 1L, 10L, 0, 1, new byte[0], new byte[] {1}, new byte[] {2}),
                record(2, 2L, 20L, 0, 1, new byte[0], new byte[] {3}, new byte[] {4})
        );
        KafkaToPscMessageIteratorConverter<byte[], byte[]> converter = converter(records, topicUri);
        TopicUriPartition partition = new TopicUriPartition(topicUri, 2);

        KafkaToPscMessageIteratorConverter<byte[], byte[]> filtered =
                (KafkaToPscMessageIteratorConverter<byte[], byte[]>) converter.iteratorFor(partition);

        assertEquals(Collections.singleton(legacyPartition(topicUri, 2)), filtered.getTopicUriPartitions());
        PscConsumerMessage<byte[], byte[]> message = filtered.getNextBackendMessage();
        assertEquals(legacyPartition(topicUri, 2), message.getMessageId().getTopicUriPartition());
        assertSame(topicUri, message.getMessageId().getTopicUriPartition().getTopicUri());
        assertEquals(2L, message.getMessageId().getOffset());
        assertFalse(filtered.hasNext());
    }

    private static KafkaToPscMessageIteratorConverter<byte[], byte[]> converter(
            List<ConsumerRecord<byte[], byte[]>> records, KafkaTopicUri topicUri
    ) {
        Map<TopicPartition, Iterator<ConsumerRecord<byte[], byte[]>>> byPartition = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            byPartition.put(topicPartition, Collections.singletonList(record).iterator());
        }
        Map<String, TopicUri> topicUris = new HashMap<>();
        topicUris.put(TOPIC, topicUri);
        return new KafkaToPscMessageIteratorConverter<>(
                records.iterator(),
                byPartition,
                byPartition.keySet(),
                topicUris,
                null
        );
    }

    private static ConsumerRecord<byte[], byte[]> record(
            int partition,
            long offset,
            long timestamp,
            int keySize,
            int valueSize,
            byte[] key,
            byte[] value,
            byte[] header
    ) {
        return new ConsumerRecord<>(
                TOPIC,
                partition,
                offset,
                timestamp,
                TimestampType.CREATE_TIME,
                keySize,
                valueSize,
                key,
                value,
                new RecordHeaders().add("h", header),
                Optional.empty()
        );
    }

    private static TopicUriPartition legacyPartition(KafkaTopicUri topicUri, int partition) {
        TopicUriPartition topicUriPartition = new TopicUriPartition(topicUri.getTopicUriAsString(), partition);
        BaseTopicUri.finalizeTopicUriPartition(topicUriPartition, topicUri);
        return topicUriPartition;
    }

    private static KafkaTopicUri kafkaTopicUri() throws TopicUriSyntaxException {
        String topicUri = "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::cluster:" + TOPIC;
        return KafkaTopicUri.validate(TopicUri.validate(topicUri));
    }
}
