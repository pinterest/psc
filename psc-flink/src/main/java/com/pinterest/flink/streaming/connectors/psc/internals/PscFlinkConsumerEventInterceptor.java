package com.pinterest.flink.streaming.connectors.psc.internals;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.event.EventHandler;
import com.pinterest.psc.common.event.PscEvent;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.interceptor.TypePreservingInterceptor;

import java.util.Collections;

public class PscFlinkConsumerEventInterceptor<K, V> extends TypePreservingInterceptor<K, V> {
  private final EventHandler eventHandler;

  public PscFlinkConsumerEventInterceptor(EventHandler eventHandler) {
    this.eventHandler = eventHandler;
  }

  @Override
  public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
    if (message.getHeaders().containsKey(PscEvent.EVENT_HEADER)) {
      String eventType = new String(message.getHeader(PscEvent.EVENT_HEADER));
      MessageId messageId = message.getMessageId();
      PscEvent event = new PscEvent(messageId.getTopicUriPartition().getTopicUri(), messageId.getTopicUriPartition(), eventType, Collections.emptyMap());
      eventHandler.handle(event);
      message.getHeaders().remove(PscEvent.EVENT_HEADER);
    }
    return super.onConsume(message);
  }
}
