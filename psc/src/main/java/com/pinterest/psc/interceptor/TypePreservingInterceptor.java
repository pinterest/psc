package com.pinterest.psc.interceptor;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.producer.PscProducerMessage;

import java.util.Collection;

public abstract class TypePreservingInterceptor<K, V>
        implements ProducerInterceptor<K, V, K, V>, ConsumerInterceptor<K, V, K, V> {

    protected PscConfigurationInternal pscConfigurationInternal;

    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
        return message;
    }

    @Override
    public PscProducerMessage<K, V> onSend(PscProducerMessage<K, V> message) {
        return message;
    }

    @Override
    public void onCommit(Collection<MessageId> messageIds) {
    }

    @VisibleForTesting
    protected TypePreservingInterceptor setPscConfigurationInternal(PscConfigurationInternal pscConfigurationInternal) {
        if (pscConfigurationInternal != null)
            this.pscConfigurationInternal = pscConfigurationInternal;
        return this;
    }
}
