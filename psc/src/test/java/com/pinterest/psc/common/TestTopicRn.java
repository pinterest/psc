package com.pinterest.psc.common;

import com.pinterest.psc.exception.startup.TopicRnSyntaxException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTopicRn {
    private final String valid_rn = TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:topic01";

    @Test
    void testTopicRnStandard() throws IOException {
        InputStream input = getClass().getClassLoader().getResourceAsStream("psc.properties");
        Properties prop = new Properties();

        // load a properties file
        prop.load(input);

        // get the property value and print it out
        assertTrue(prop.containsKey("topic.rn.standard"));
        assertEquals(TopicUri.STANDARD, prop.getProperty("topic.rn.standard"));
    }

    @Test
    void testTopicRn() throws Exception {
        // bad cases:
        // 1. non matching
        assertThrows(TopicRnSyntaxException.class, () -> TopicRn.validate("nonmatching"));

        // 2. fewer parts
        assertThrows(TopicRnSyntaxException.class, () -> TopicRn.validate("ab:cd"));

        // 3. more parts
        assertThrows(TopicRnSyntaxException.class, () -> TopicRn.validate("ab:cd:ef:gh:ij:kl:mn:op:qr"));

        // 4. empty parts
        assertThrows(TopicRnSyntaxException.class, () -> TopicRn.validate("std:par:svc:rgn:act::t"));

        // 5. invalid chars
        assertThrows(TopicRnSyntaxException.class, () -> TopicRn.validate("std?:par:svc#:rgn+:act::t"));

        TopicRn.validate(valid_rn);
    }
}