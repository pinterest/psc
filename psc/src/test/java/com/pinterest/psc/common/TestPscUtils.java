package com.pinterest.psc.common;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPscUtils {
    private static final Random random = new Random();

    @Test
    void testIntByteArrayConversions() {
        assertEquals(0, PscCommon.byteArrayToInt(PscCommon.intToByteArray(0)));
        assertEquals(1, PscCommon.byteArrayToInt(PscCommon.intToByteArray(1)));
        assertEquals(-1, PscCommon.byteArrayToInt(PscCommon.intToByteArray(-1)));
        assertEquals(Integer.MAX_VALUE, PscCommon.byteArrayToInt(PscCommon.intToByteArray(Integer.MAX_VALUE)));
        assertEquals(Integer.MIN_VALUE, PscCommon.byteArrayToInt(PscCommon.intToByteArray(Integer.MIN_VALUE)));

        for (int i = 0; i < 100; ++i) {
            int num = random.nextInt();
            assertEquals(num, PscCommon.byteArrayToInt(PscCommon.intToByteArray(num)));
            assertEquals(-num, PscCommon.byteArrayToInt(PscCommon.intToByteArray(-num)));
        }
    }

    @Test
    void testLongByteArrayConversions() {
        assertEquals(0L, PscCommon.byteArrayToLong(PscCommon.longToByteArray(0L)));
        assertEquals(1L, PscCommon.byteArrayToLong(PscCommon.longToByteArray(1L)));
        assertEquals(-1L, PscCommon.byteArrayToLong(PscCommon.longToByteArray(-1L)));
        assertEquals(Long.MAX_VALUE, PscCommon.byteArrayToLong(PscCommon.longToByteArray(Long.MAX_VALUE)));
        assertEquals(Long.MIN_VALUE, PscCommon.byteArrayToLong(PscCommon.longToByteArray(Long.MIN_VALUE)));

        for (int i = 0; i < 100; ++i) {
            long num = random.nextLong();
            assertEquals(num, PscCommon.byteArrayToLong(PscCommon.longToByteArray(num)));
            assertEquals(-num, PscCommon.byteArrayToLong(PscCommon.longToByteArray(-num)));
        }
    }
}
