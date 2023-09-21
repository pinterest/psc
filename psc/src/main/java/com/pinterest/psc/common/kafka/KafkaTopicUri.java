package com.pinterest.psc.common.kafka;

import com.pinterest.psc.common.BaseTopicUri;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;

public class KafkaTopicUri extends BaseTopicUri {
    public static final String PLAINTEXT_PROTOCOL = "plaintext";
    public static final String SECURE_PROTOCOL = "secure";

    public KafkaTopicUri(TopicUri baseTopicUri) {
        super(baseTopicUri);
    }

    public static KafkaTopicUri validate(TopicUri baseTopicUri) throws TopicUriSyntaxException {
        if (baseTopicUri.getTopicRn() == null)
            throw new TopicUriSyntaxException(baseTopicUri.getTopicUriAsString(), "Missing topic RN in topic URI");
        if (!baseTopicUri.getTopicRn().getService().equals(PscUtils.BACKEND_TYPE_KAFKA))
            throw new TopicUriSyntaxException(baseTopicUri.getTopicUriAsString(), "Not a Kafka URI");
        if (baseTopicUri.getProtocol() != null &&
                !baseTopicUri.getProtocol().equals(PLAINTEXT_PROTOCOL) &&
                !baseTopicUri.getProtocol().equals(SECURE_PROTOCOL)) {
            throw new TopicUriSyntaxException(
                    baseTopicUri.getTopicUriAsString(), "Invalid protocol in Kafka Topic URI: '" +
                    baseTopicUri.getProtocol() + "'. " + "Supported protocols are '" + SECURE_PROTOCOL +
                    "' and '" + PLAINTEXT_PROTOCOL + "'"
            );
        }
        return new KafkaTopicUri(baseTopicUri);
    }
}
