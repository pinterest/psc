package com.pinterest.psc.common;

import com.pinterest.psc.exception.startup.TopicRnSyntaxException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestBaseTopicUri {
    private final String valid_rn = TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:topic01";

    @Test
    void testBaseTopicUri() throws Exception {
        Exception exception;

        // bad cases:
        // 1. matches URI schema, but not RN schema (default protocol)
        exception = assertThrows(TopicUriSyntaxException.class, () -> TopicUri.validate("nonmatching"));
        assertEquals(TopicRnSyntaxException.class, exception.getCause().getClass());

        // 2. matches URI schema, but not RN schema (protocol as part of RN)
        exception = assertThrows(TopicUriSyntaxException.class, () -> TopicUri.validate("protocol:" + valid_rn));
        assertEquals(TopicRnSyntaxException.class, exception.getCause().getClass());

        // 3. no colon, not matching URI schema
        exception = assertThrows(TopicUriSyntaxException.class, () -> TopicUri.validate("protocol/" + valid_rn));
        assertNull(exception.getCause());

        // 4. no rn, not matching URI schema
        exception = assertThrows(TopicUriSyntaxException.class, () -> TopicUri.validate("protocol:/"));
        assertNull(exception.getCause());

        // 5. no protocol, not matching URI schema
        exception = assertThrows(TopicUriSyntaxException.class, () -> TopicUri.validate(":/" + valid_rn));
        assertNull(exception.getCause());

        // 6. no protocol or rn, not matching URI schema
        exception = assertThrows(TopicUriSyntaxException.class, () -> TopicUri.validate(":/"));
        assertNull(exception.getCause());

        // 7. two slashes, not matching URI schema
        exception = assertThrows(TopicUriSyntaxException.class, () -> TopicUri.validate("protocol://" + valid_rn));
        assertNull(exception.getCause());

        // 8. more slashes, not matching URI schema
        exception = assertThrows(TopicUriSyntaxException.class, () -> TopicUri.validate("protocol:///" + valid_rn));
        assertNull(exception.getCause());

        // 9. no protocol or prefix is ok
        TopicUri.validate(valid_rn);

        // 10. full uri is ok
        TopicUri.validate("protocol:" + TopicUri.SEPARATOR + valid_rn);
    }

    @Test
    void testValidation() throws TopicUriSyntaxException {
        String topicUri1 = TopicUri.DEFAULT_PROTOCOL + ":" + TopicUri.SEPARATOR + valid_rn;
        assertEquals(topicUri1, TopicUri.validate(topicUri1).getTopicUriAsString());

        String topicUri2 = "protocol:" + TopicUri.SEPARATOR + valid_rn;
        assertEquals(topicUri2, TopicUri.validate(topicUri2).getTopicUriAsString());

        String topicUri3 = "secure:" + TopicUri.SEPARATOR + valid_rn;
        assertEquals(topicUri3, TopicUri.validate(topicUri3).getTopicUriAsString());
    }
}