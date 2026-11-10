package com.pinterest.psc.common;

import com.pinterest.psc.common.kafka.KafkaMessageId;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.consumer.memq.MemqTopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The producer and consumer used to build a {@link TopicUriPartition} from a URI string, which parses the URI, and
 * then overwrite that parse with a {@link TopicUri} they already had. These tests lock the observable result of that
 * older construction against the constructors that skip the second parse.
 */
class TestTopicUriPartition {
    private static final int[] PARTITIONS = {PscUtils.NO_PARTITION, 0, 1, Integer.MAX_VALUE};

    @Test
    void topicUriConstructorMatchesStringConstructorPlusFinalize() throws Exception {
        for (TopicUri topicUri : validatedTopicUris()) {
            for (int partition : PARTITIONS) {
                TopicUriPartition current = new TopicUriPartition(topicUri, partition);
                TopicUriPartition legacy = legacyFinalized(topicUri.getTopicUriAsString(), partition, topicUri);
                assertEquivalent(legacy, current);
                assertSame(topicUri, current.getTopicUri());
            }
        }
    }

    @Test
    void explicitStringConstructorMatchesStringConstructorPlusFinalize() throws Exception {
        for (TopicUri topicUri : validatedTopicUris()) {
            String callerString = topicUri.getTopicUriAsString();
            String rawString = callerString.substring(callerString.indexOf(TopicUri.SEPARATOR) + TopicUri.SEPARATOR.length());
            for (String topicUriString : Arrays.asList(callerString, rawString)) {
                for (int partition : PARTITIONS) {
                    TopicUriPartition current = new TopicUriPartition(topicUriString, partition, topicUri);
                    TopicUriPartition legacy = legacyFinalized(topicUriString, partition, topicUri);
                    assertEquivalent(legacy, current);
                    assertSame(topicUri, current.getTopicUri());
                    assertEquals(topicUriString, current.getTopicUriAsString());
                }
            }
        }
    }

    @Test
    void callerStringThatIsNotCanonicalStaysDistinctFromTheCanonicalPartition() throws Exception {
        TopicUri topicUri = kafkaTopicUri("plaintext:" + TopicUri.SEPARATOR + kafkaRn("topic01"));
        String rawRn = kafkaRn("topic01");
        TopicUriPartition fromCallerString = new TopicUriPartition(rawRn, 2, topicUri);
        TopicUriPartition fromTopicUri = new TopicUriPartition(topicUri, 2);

        assertEquals(rawRn, fromCallerString.getTopicUriAsString());
        assertEquals(topicUri.getTopicUriAsString(), fromTopicUri.getTopicUriAsString());
        assertNotEquals(fromCallerString, fromTopicUri);
        assertSame(topicUri, fromCallerString.getTopicUri());
        assertSame(topicUri, fromTopicUri.getTopicUri());

        Map<TopicUriPartition, Integer> partitions = new HashMap<>();
        partitions.put(fromCallerString, 1);
        partitions.put(fromTopicUri, 2);
        assertEquals(2, partitions.size());
        assertEquals(Integer.valueOf(1), partitions.get(legacyFinalized(rawRn, 2, topicUri)));
        assertEquals(Integer.valueOf(2), partitions.get(legacyFinalized(topicUri.getTopicUriAsString(), 2, topicUri)));
    }

    @Test
    void partitionsAreInterchangeableAsSetAndMapKeys() throws Exception {
        TopicUri topicUri = kafkaTopicUri("secure:" + TopicUri.SEPARATOR + kafkaRn("topic01"));
        Set<TopicUriPartition> partitions = new HashSet<>();
        Map<TopicUriPartition, Long> offsets = new HashMap<>();
        for (int partition : PARTITIONS) {
            partitions.add(legacyFinalized(topicUri.getTopicUriAsString(), partition, topicUri));
            offsets.put(new TopicUriPartition(topicUri, partition), (long) partition);
        }

        for (int partition : PARTITIONS) {
            assertTrue(partitions.contains(new TopicUriPartition(topicUri, partition)));
            assertTrue(partitions.contains(new TopicUriPartition(topicUri.getTopicUriAsString(), partition, topicUri)));
            assertEquals(Long.valueOf(partition), offsets.get(legacyFinalized(topicUri.getTopicUriAsString(), partition, topicUri)));
        }
        assertEquals(PARTITIONS.length, partitions.size());
    }

    @Test
    void differentPartitionOrTopicIsNotEqual() throws Exception {
        TopicUri first = kafkaTopicUri("plaintext:" + TopicUri.SEPARATOR + kafkaRn("topic01"));
        TopicUri second = kafkaTopicUri("plaintext:" + TopicUri.SEPARATOR + kafkaRn("topic02"));
        TopicUriPartition partition = new TopicUriPartition(first, 1);

        assertNotEquals(partition, new TopicUriPartition(first, 2));
        assertNotEquals(partition, new TopicUriPartition(second, 1));
        assertNotEquals(partition.hashCode(), new TopicUriPartition(first, 2).hashCode());
        assertNotEquals(0, partition.compareTo(new TopicUriPartition(second, 1)));
    }

    @Test
    void messageIdBuiltFromEitherConstructorComparesEqual() throws Exception {
        TopicUri topicUri = kafkaTopicUri("plaintext:" + TopicUri.SEPARATOR + kafkaRn("topic01"));
        TopicUriPartition current = new TopicUriPartition(topicUri.getTopicUriAsString(), 4, topicUri);
        TopicUriPartition legacy = legacyFinalized(topicUri.getTopicUriAsString(), 4, topicUri);

        KafkaMessageId currentMessageId = new KafkaMessageId(current, 90L, 1_000L, 2, 8);
        KafkaMessageId legacyMessageId = new KafkaMessageId(legacy, 90L, 1_000L, 2, 8);

        assertEquals(legacyMessageId, currentMessageId);
        assertEquals(legacyMessageId.toString(), currentMessageId.toString());
        assertEquals(90L, currentMessageId.getOffset());
        assertEquals(1_000L, currentMessageId.getTimestamp());
        assertEquals(2, currentMessageId.getSerializedKeySizeBytes());
        assertEquals(8, currentMessageId.getSerializedValueSizeBytes());
    }

    @Test
    void serializationRoundTripPreservesTheFinalizedPartition() throws Exception {
        for (TopicUri topicUri : validatedTopicUris()) {
            TopicUriPartition current = new TopicUriPartition("caller-preserved:" + topicUri.getTopicUriAsString(), 6, topicUri);
            TopicUriPartition legacy = legacyFinalized(current.getTopicUriAsString(), 6, topicUri);

            TopicUriPartition currentRestored = roundTrip(current);
            TopicUriPartition legacyRestored = roundTrip(legacy);

            assertEquivalent(legacyRestored, currentRestored);
            assertEquivalent(current, currentRestored);
            assertEquals(topicUri, currentRestored.getTopicUri());
            assertEquals(topicUri.getClass(), currentRestored.getTopicUri().getClass());
        }
    }

    @Test
    void stringConstructorStillParsesAndStillDropsAnInvalidUri() throws Exception {
        String canonical = "plaintext:" + TopicUri.SEPARATOR + kafkaRn("topic01");
        TopicUriPartition parsed = new TopicUriPartition(canonical, 0);

        assertEquals(canonical, parsed.getTopicUriAsString());
        assertEquals(BaseTopicUri.class, parsed.getTopicUri().getClass());
        assertEquals(TopicUri.validate(canonical), parsed.getTopicUri());
        assertEquals(0, parsed.getPartition());

        TopicUriPartition invalid = new TopicUriPartition("not a uri", 3);
        assertNull(invalid.getTopicUri());
        assertEquals("not a uri", invalid.getTopicUriAsString());
        assertEquals(3, invalid.getPartition());
        assertTrue(invalid.toString().contains("Finalized?: false"));
    }

    @Test
    void explicitStringConstructorDoesNotParseTheString() throws Exception {
        TopicUri topicUri = kafkaTopicUri("plaintext:" + TopicUri.SEPARATOR + kafkaRn("topic01"));
        TopicUriPartition partition = new TopicUriPartition("not a uri", 9, topicUri);

        assertSame(topicUri, partition.getTopicUri());
        assertEquals("not a uri", partition.getTopicUriAsString());
        assertEquals(9, partition.getPartition());
        assertNotEquals(new TopicUriPartition("not a uri", 9), partition);
    }

    private static void assertEquivalent(TopicUriPartition expected, TopicUriPartition actual) {
        assertEquals(expected, actual);
        assertEquals(actual, expected);
        assertEquals(expected.hashCode(), actual.hashCode());
        assertEquals(0, expected.compareTo(actual));
        assertEquals(0, actual.compareTo(expected));
        assertEquals(expected.getTopicUriAsString(), actual.getTopicUriAsString());
        assertEquals(expected.getPartition(), actual.getPartition());
        assertEquals(expected.getTopicUri(), actual.getTopicUri());
        assertEquals(expected.toString(), actual.toString());
        assertFalse(expected == actual);
    }

    private static TopicUriPartition legacyFinalized(String topicUriString, int partition, TopicUri topicUri) {
        TopicUriPartition partitionObject = new TopicUriPartition(topicUriString, partition);
        BaseTopicUri.finalizeTopicUriPartition(partitionObject, topicUri);
        return partitionObject;
    }

    private static TopicUri[] validatedTopicUris() throws TopicUriSyntaxException {
        return new TopicUri[] {
                kafkaTopicUri("plaintext:" + TopicUri.SEPARATOR + kafkaRn("topic01")),
                kafkaTopicUri("secure:" + TopicUri.SEPARATOR + kafkaRn("topic02")),
                memqTopicUri("plaintext:" + TopicUri.SEPARATOR + memqRn("topic03")),
                memqTopicUri("secure:" + TopicUri.SEPARATOR + memqRn("topic04"))
        };
    }

    private static KafkaTopicUri kafkaTopicUri(String topicUri) throws TopicUriSyntaxException {
        return KafkaTopicUri.validate(TopicUri.validate(topicUri));
    }

    private static MemqTopicUri memqTopicUri(String topicUri) throws TopicUriSyntaxException {
        return MemqTopicUri.validate(TopicUri.validate(topicUri));
    }

    private static String kafkaRn(String topic) {
        return TopicUri.STANDARD + ":kafka:env:aws_us-west-1::cluster:" + topic;
    }

    private static String memqRn(String topic) {
        return TopicUri.STANDARD + ":memq:env:aws_us-west-1::cluster:" + topic;
    }

    private static TopicUriPartition roundTrip(TopicUriPartition partition) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(partition);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (TopicUriPartition) in.readObject();
        }
    }
}
