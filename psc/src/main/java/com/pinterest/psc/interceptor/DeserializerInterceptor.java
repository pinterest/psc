package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.PscMessageTags;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.exception.consumer.KeyDeserializerException;
import com.pinterest.psc.exception.consumer.ValueDeserializerException;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.serde.Deserializer;
import com.pinterest.psc.logging.PscLogger;

import java.util.HashSet;
import java.util.Set;

public class DeserializerInterceptor<K, V> implements ConsumerInterceptor<byte[], byte[], K, V> {
    private static final PscLogger logger = PscLogger.getLogger(DeserializerInterceptor.class);
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private PscConfigurationInternal pscConfigurationInternal;

    public DeserializerInterceptor(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<byte[], byte[]> message) {
        Set<PscMessageTags> tags = new HashSet<>();
        K pscKey = null;
        try {
            pscKey = keyDeserializer.deserialize(message.getKey());
        } catch (DeserializerException e) {
            logger.error(
                    String.format("Failed to deserialize key of message %s", message.getMessageId()), e
            );
            tags.add(PscConsumerMessage.DefaultPscConsumerMessageTags.KEY_DESERIALIZATION_FAILED);
            PscErrorHandler.handle(
                    new KeyDeserializerException(e), message.getMessageId().getTopicUriPartition().getTopicUri(), true,
                    pscConfigurationInternal
            );
        }

        V pscValue = null;
        try {
            pscValue = valueDeserializer.deserialize(message.getValue());
        } catch (DeserializerException e) {
            logger.error(
                    String.format("Failed to deserialize value of message %s", message.getMessageId()), e
            );
            tags.add(PscConsumerMessage.DefaultPscConsumerMessageTags.VALUE_DESERIALIZATION_FAILED);
            PscErrorHandler.handle(
                    new ValueDeserializerException(e), message.getMessageId().getTopicUriPartition().getTopicUri(), true,
                    pscConfigurationInternal
            );
        }

        PscConsumerMessage<K, V> pscConsumerMessage = new PscConsumerMessage<>(
                message.getMessageId(),
                pscKey,
                pscValue,
                message.getPublishTimestamp()
        );

        pscConsumerMessage.setHeaders(message.getHeaders());
        pscConsumerMessage.setTags(tags);
        return pscConsumerMessage;
    }

    protected DeserializerInterceptor setPscConfigurationInternal(PscConfigurationInternal pscConfigurationInternal) {
        if (this.pscConfigurationInternal != null) {
            this.pscConfigurationInternal = pscConfigurationInternal;
        }
        return this;
    }
}
