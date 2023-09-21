package com.pinterest.psc.interceptor;

import com.pinterest.psc.consumer.PscConsumerMessage;

public class ModifierInterceptor<K, V> extends IdentityInterceptor<K, V> {
    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
        super.onConsume(message);
        return message;
    }
}
