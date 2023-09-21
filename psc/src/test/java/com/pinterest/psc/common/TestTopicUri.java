package com.pinterest.psc.common;

import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

public class TestTopicUri extends KafkaTopicUri {
    public TestTopicUri(TopicUri baseTopicUri) {
        super(baseTopicUri);
    }

    public static TestTopicUri validate(TopicUri baseTopicUri) throws TopicUriSyntaxException {
        if (baseTopicUri.getTopicRn() == null)
            throw new TopicUriSyntaxException(baseTopicUri.getTopicUriAsString(), "Missing path in Test URI");
        if (!baseTopicUri.getBackend().equals(TestUtils.BACKEND_TYPE_TEST))
            throw new TopicUriSyntaxException(baseTopicUri.getTopicUriAsString(), "Not a Test URI");
        if (baseTopicUri.getProtocol() != null &&
                !baseTopicUri.getProtocol().equals(PLAINTEXT_PROTOCOL) &&
                !baseTopicUri.getProtocol().equals(SECURE_PROTOCOL)) {
            throw new TopicUriSyntaxException(
                    baseTopicUri.getTopicUriAsString(), "Invalid protocol in Test URI: '" +
                    baseTopicUri.getProtocol() + "'. " + "Supported protocols are '" + SECURE_PROTOCOL +
                    "' and '" + PLAINTEXT_PROTOCOL + "'"
            );
        }
        return new TestTopicUri(baseTopicUri);
    }
}
