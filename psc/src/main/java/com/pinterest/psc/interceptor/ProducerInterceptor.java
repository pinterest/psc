package com.pinterest.psc.interceptor;

import com.pinterest.psc.interceptor.Interceptor;
import com.pinterest.psc.producer.PscProducerMessage;

public interface ProducerInterceptor<K1, V1, K2, V2> extends Interceptor<K1, V1, K2, V2> {
    /**
     * This interceptor is invoked upon producer send calls on each PscProducer message. The message will run through
     * all default and configured typed interceptors, serializer interceptor, and raw interceptors one by one.
     *
     * @param message The PscProducer message or a transformed version of it as it goes through the interceptors.
     * @return A PSC producer message that, depending on the interceptor type, may or may not be the same as the input
     * message.
     */
    PscProducerMessage<K2, V2> onSend(PscProducerMessage<K1, V1> message);
}
