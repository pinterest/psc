package com.pinterest.psc.common;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPscCommon {

    @RepeatedTest(10)
    void testIntAndByteArrayConversion() {
        int n = new Random().nextInt();
        assertEquals(n, PscCommon.byteArrayToInt(PscCommon.intToByteArray(n)));

        byte[] bytes = PscCommon.intToByteArray(n);
        assertTrue(PscCommon.equals(bytes, PscCommon.intToByteArray(PscCommon.byteArrayToInt(bytes))));
    }

    @RepeatedTest(10)
    void testLongAndByteArrayConversion() {
        long n = new Random().nextLong();
        assertEquals(n, PscCommon.byteArrayToLong(PscCommon.longToByteArray(n)));

        byte[] bytes = PscCommon.longToByteArray(n);
        assertTrue(PscCommon.equals(bytes, PscCommon.longToByteArray(PscCommon.byteArrayToLong(bytes))));
    }

    @Test
    void testStringAndByteArrayConversion() {
        String s = getRandomString(1000);
        assertEquals(s, PscCommon.byteArrayToString(PscCommon.stringToByteArray(s)));

        byte[] bytes = PscCommon.stringToByteArray(s);
        assertTrue(PscCommon.equals(bytes, PscCommon.stringToByteArray(PscCommon.byteArrayToString(bytes))));
    }

    @Test
    void testByteArrayComparison() {
        byte[] bytes1 = new byte[] {};
        byte[] bytes2 = new byte[] {1};
        byte[] bytes3 = new byte[] {2};
        byte[] bytes4 = new byte[] {1, 2, 3, 4};
        byte[] bytes5 = new byte[] {1, 2, 3, 5};

        assertEquals(0, PscCommon.compare(null, null));
        assertTrue(PscCommon.compare(null, bytes1) < 0);
        assertTrue(PscCommon.compare(bytes1, null) > 0);
        assertEquals(0, PscCommon.compare(bytes1, bytes1));
        assertEquals(0, PscCommon.compare(bytes2, bytes2));
        assertTrue(PscCommon.compare(bytes1, bytes2) < 0);
        assertTrue(PscCommon.compare(bytes2, bytes3) < 0);
        assertTrue(PscCommon.compare(bytes2, bytes4) < 0);
        assertTrue(PscCommon.compare(bytes4, bytes5) < 0);
    }

    private String getRandomString(int maxLength) {
        int length = new Random().nextInt(maxLength);
        byte[] array = new byte[length + 1];
        new Random().nextBytes(array);
        return new String(array, StandardCharsets.UTF_8);
    }
}