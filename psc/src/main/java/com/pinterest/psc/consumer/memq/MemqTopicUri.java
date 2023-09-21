package com.pinterest.psc.consumer.memq;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

public class MemqTopicUri extends BaseTopicUri {
    public static final String PLAINTEXT_PROTOCOL = "plaintext";
    public static final String SECURE_PROTOCOL = "secure";

    MemqTopicUri(TopicUri topicUri) {
        super(topicUri);
    }

    public static MemqTopicUri validate(TopicUri baseTopicUri) throws TopicUriSyntaxException {
        if (baseTopicUri.getTopicRn() == null)
            throw new TopicUriSyntaxException(baseTopicUri.getTopicUriAsString(),
                    "Missing topic RN in Memq URI");
        if (!baseTopicUri.getTopicRn().getService().equals(PscUtils.BACKEND_TYPE_MEMQ))
            throw new TopicUriSyntaxException(baseTopicUri.getTopicUriAsString(), "Not a MemQ URI");
        if (baseTopicUri.getProtocol() != null &&
                !baseTopicUri.getProtocol().equals(PLAINTEXT_PROTOCOL) &&
                !baseTopicUri.getProtocol().equals(SECURE_PROTOCOL)) {
            throw new TopicUriSyntaxException(
                    baseTopicUri.getTopicUriAsString(), "Invalid protocol in MemQ Topic URI: '" +
                    baseTopicUri.getProtocol() + "'. " + "Supported protocols are '" + SECURE_PROTOCOL +
                    "' and '" + PLAINTEXT_PROTOCOL + "'"
            );
        }
        return new MemqTopicUri(baseTopicUri);
    }
}
