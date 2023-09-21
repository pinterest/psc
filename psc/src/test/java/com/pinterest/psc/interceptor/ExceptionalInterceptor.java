package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.consumer.PscConsumerMessage;

import java.util.Collection;

public class ExceptionalInterceptor<K, V> extends IdentityInterceptor<K, V> {

    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
        super.onConsume(message);
        throw new RuntimeException("exception");
    }

    @Override
    public void onCommit(Collection<MessageId> messageIds) {
        super.onCommit(messageIds);
        throw new RuntimeException("exception");
    }

    /*
    @Override
    public void close() {
      super.close();
      throw new RuntimeException("exception");
    }
     */
}
