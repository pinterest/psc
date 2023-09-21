package com.pinterest.psc.common;

import com.pinterest.psc.metrics.NullMetricsReporter;

import java.util.Random;

public class TestUtils {
    public static final String BACKEND_TYPE_TEST = "test";
    public static final String DEFAULT_METRICS_REPORTER = NullMetricsReporter.class.getName();
    protected static final Random random = new Random();

    public static byte[] getRandomBytes(int size) {
        return getRandomBytes(size, 0);
    }

    public static byte[] getRandomBytes(int size, double chanceOfNull) {
        if (random.nextDouble() < chanceOfNull)
            return null;

        byte[] bytes = new byte[random.nextInt(256)];
        random.nextBytes(bytes);
        return bytes;
    }

    public static String getRandomString(int size) {
        return getRandomString(size, 0);
    }

    public static String getRandomString(int size, boolean alphabeticalOnly) {
        return getRandomString(size, 0, alphabeticalOnly);
    }

    public static String getRandomString(int size, double chanceOfNull) {
        return getRandomString(size, chanceOfNull, false);
    }

    public static String getRandomString(int size, double chanceOfNull, boolean alphabeticalOnly) {
        if (random.nextDouble() < chanceOfNull)
            return null;

        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int alphabetLength = alphabet.length();

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < size; ++i) {
            stringBuilder.append(alphabeticalOnly ?
                    alphabet.charAt(random.nextInt(alphabetLength)) :
                    (char) random.nextInt(256));
        }

        return stringBuilder.toString();
    }

    public static TopicUriPartition getFinalizedTopicUriPartition(TopicUri topicUri, int partition) {
        return new TopicUriPartition(topicUri, partition);
    }
}
