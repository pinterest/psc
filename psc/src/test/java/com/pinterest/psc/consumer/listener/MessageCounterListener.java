package com.pinterest.psc.consumer.listener;

import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;

public class MessageCounterListener<K, V> implements MessageListener<K, V> {
    private int handleCallCounter = 0;

    @Override
    public void handle(PscConsumer<K, V> consumer, PscConsumerMessage<K, V> message) {
        ++handleCallCounter;
    }

    public int getHandleCallCounter() {
        return handleCallCounter;
    }
}
