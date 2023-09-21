package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.exception.handler.PscErrorHandler;

import java.util.Collection;

public class ExceptionalMetricsReportingInterceptor<K, V> extends IdentityInterceptor<K, V> {

    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
        try {
            super.onConsume(message);
            throw new DeserializerException("deser exception");
        } catch (DeserializerException e) {
            PscErrorHandler.handle(
                    e,
                    message.getMessageId().getTopicUriPartition().getTopicUri(),
                    true,
                    pscConfigurationInternal
            );
        }
        return message;
    }

    @Override
    public void onCommit(Collection<MessageId> messageIds) {
        super.onCommit(messageIds);
    }
}
