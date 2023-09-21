package com.pinterest.psc.consumer;

import com.pinterest.psc.interceptor.ConsumerInterceptors;

public abstract class ToPscMessageIteratorConverter<K, V> extends PscConsumerPollMessageIterator<K, V> {
    protected final ConsumerInterceptors<K, V> consumerInterceptors;

    public ToPscMessageIteratorConverter(ConsumerInterceptors<K, V> consumerInterceptors) {
        this.consumerInterceptors = consumerInterceptors;
    }

    public final PscConsumerMessage<K, V> next() {
        return consumerInterceptors.onConsume(getNextBackendMessage());
    }

    protected abstract PscConsumerMessage<byte[], byte[]> getNextBackendMessage();
}