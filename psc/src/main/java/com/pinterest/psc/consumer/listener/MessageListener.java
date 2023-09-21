package com.pinterest.psc.consumer.listener;

import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;

public interface MessageListener<K, V> {
    void handle(PscConsumer<K, V> consumer, PscConsumerMessage<K, V> message);
}
