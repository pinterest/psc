package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.PscMessageTags;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.exception.producer.KeySerializerException;
import com.pinterest.psc.exception.producer.SerializerException;
import com.pinterest.psc.exception.producer.ValueSerializerException;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.Serializer;
import com.pinterest.psc.logging.PscLogger;

import java.util.HashSet;
import java.util.Set;

public class SerializerInterceptor<K, V> implements ProducerInterceptor<K, V, byte[], byte[]> {
    private static final PscLogger logger = PscLogger.getLogger(SerializerInterceptor.class);
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private PscConfigurationInternal pscConfigurationInternal;

    public SerializerInterceptor(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public PscProducerMessage<byte[], byte[]> onSend(PscProducerMessage<K, V> pscProducerMessage) {
        Set<PscMessageTags> tags = new HashSet<>();
        byte[] pscKey = null;
        try {
            pscKey = keySerializer.serialize(pscProducerMessage.getKey());
        } catch (SerializerException e) {
            logger.error(String.format("Failed to serialize key of message: %s", pscProducerMessage), e);
            tags.add(PscProducerMessage.DefaultPscProducerMessageTags.KEY_SERIALIZATION_FAILED);
            PscErrorHandler.handle(
                    new KeySerializerException(e), pscProducerMessage.getTopicUriPartition().getTopicUri(), true,
                    pscConfigurationInternal
            );

        }

        byte[] pscValue = null;
        try {
            pscValue = valueSerializer.serialize(pscProducerMessage.getValue());
        } catch (SerializerException e) {
            logger.error(String.format("Failed to serialize value of message %s ", pscProducerMessage), e);
            tags.add(PscProducerMessage.DefaultPscProducerMessageTags.VALUE_SERIALIZATION_FAILED);
            PscErrorHandler.handle(
                    new ValueSerializerException(e), pscProducerMessage.getTopicUriPartition().getTopicUri(), true,
                    pscConfigurationInternal
            );
        }

        PscProducerMessage<byte[], byte[]> rawProducerMessage = new PscProducerMessage<>(pscProducerMessage, pscKey, pscValue);
        tags.forEach(rawProducerMessage::addTag);
        return rawProducerMessage;
    }

    protected SerializerInterceptor setPscConfigurationInternal(PscConfigurationInternal pscConfigurationInternal) {
        if (this.pscConfigurationInternal != null) {
            this.pscConfigurationInternal = pscConfigurationInternal;
        }
        return this;
    }
}
