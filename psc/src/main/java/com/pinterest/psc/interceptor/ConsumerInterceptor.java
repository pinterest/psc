package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.interceptor.Interceptor;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;

public interface ConsumerInterceptor<K1, V1, K2, V2> extends Interceptor<K1, V1, K2, V2> {
    /**
     * This interceptor is invoked after consumption of each message and its conversion to a PSC message format.
     * The message will run through all default and configured raw interceptors, deserializer interceptor, and types
     * interceptors one by one.
     *
     * @param message The PscConsumer message that is created from the backend consumer message to return to client.
     * @return A PSC consumer message that, depending on the interceptor type, may or may not be the same as the input
     * message.
     */
    PscConsumerMessage<K2, V2> onConsume(PscConsumerMessage<K1, V1> message);

    /**
     * This interceptor is invoked upon consumer commit call where specific message ids are involved. Note that,
     * at this time, consumer calls to <code>commitSync()</code> and <code>commitAsync()</code> (with no argument) do
     * not trigger this interceptor.
     *
     * @param messageIds The message is whose offset is being committed.
     */
    @InterfaceStability.Evolving
    default void onCommit(Collection<MessageId> messageIds) {
    }
}
