package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.interceptor.TypePreservingInterceptor;

import java.util.Collection;

public class IdentityInterceptor<K, V> extends TypePreservingInterceptor<K, V> {
    public int configureCounter = 0;
    public int onConsumeCounter = 0;
    public int onCommitCounter = 0;
    public int closeCounter = 0;

    /*
    @Override
    public void close() {
      closeCounter++;
    }
     */

    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
        onConsumeCounter++;
        return message;
    }

    @Override
    public void onCommit(Collection<MessageId> messageIds) {
        onCommitCounter++;
    }

    /*
    @Override
    public void configure(PscConsumerConfig<K, V> config) {
      configureCounter++;
    }
     */
}
