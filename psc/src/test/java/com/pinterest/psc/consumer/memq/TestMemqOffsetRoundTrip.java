package com.pinterest.psc.consumer.memq;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that raw Kafka notification offsets survive a round-trip through
 * the composite MemqOffset encoding used by PscMemqConsumer.
 *
 * Bug context: offsetsForTimes / endOffsets / startOffsets were returning raw
 * Kafka offsets, but seekToOffset decodes them as composite MemqOffsets
 * (bit-shifting right by 19). Without wrapping via kafkaOffsetToComposite,
 * a raw offset like 2205646 would be decoded as batch=4 instead of batch=2205646.
 */
public class TestMemqOffsetRoundTrip {

    @Test
    public void testRawKafkaOffsetRoundTrips() {
        long rawKafkaOffset = 2205646L;

        long composite = new MemqOffset(rawKafkaOffset, 0).toLong();
        MemqOffset decoded = MemqOffset.convertPscOffsetToMemqOffset(composite);

        assertEquals(rawKafkaOffset, decoded.getBatchOffset());
        assertEquals(0, decoded.getMessageOffset());
    }

    @Test
    public void testRawOffsetWithoutEncodingIsCorrupted() {
        long rawKafkaOffset = 2205646L;

        // Decoding a raw offset directly (the old bug) produces wrong batch offset
        MemqOffset decoded = MemqOffset.convertPscOffsetToMemqOffset(rawKafkaOffset);

        // 2205646 >>> 19 = 4, not 2205646
        assertEquals(4, decoded.getBatchOffset(),
                "Raw offset decoded without encoding should lose upper bits");
    }

    @Test
    public void testSmallOffsetRoundTrips() {
        long rawKafkaOffset = 42L;

        long composite = new MemqOffset(rawKafkaOffset, 0).toLong();
        MemqOffset decoded = MemqOffset.convertPscOffsetToMemqOffset(composite);

        assertEquals(rawKafkaOffset, decoded.getBatchOffset());
        assertEquals(0, decoded.getMessageOffset());
    }

    @Test
    public void testZeroOffsetRoundTrips() {
        long composite = new MemqOffset(0, 0).toLong();
        MemqOffset decoded = MemqOffset.convertPscOffsetToMemqOffset(composite);

        assertEquals(0, decoded.getBatchOffset());
        assertEquals(0, decoded.getMessageOffset());
    }

    @Test
    public void testLargeOffsetRoundTrips() {
        long rawKafkaOffset = 10_000_000L;

        long composite = new MemqOffset(rawKafkaOffset, 0).toLong();
        MemqOffset decoded = MemqOffset.convertPscOffsetToMemqOffset(composite);

        assertEquals(rawKafkaOffset, decoded.getBatchOffset());
        assertEquals(0, decoded.getMessageOffset());
    }

    @Test
    public void testCompositeWithMessageOffsetRoundTrips() {
        long batchOffset = 500L;
        int messageOffset = 1234;

        long composite = new MemqOffset(batchOffset, messageOffset).toLong();
        MemqOffset decoded = MemqOffset.convertPscOffsetToMemqOffset(composite);

        assertEquals(batchOffset, decoded.getBatchOffset());
        assertEquals(messageOffset, decoded.getMessageOffset());
    }
}
