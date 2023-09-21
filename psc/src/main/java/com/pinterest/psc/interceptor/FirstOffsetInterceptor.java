package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.logging.PscLogger;

import java.util.HashSet;
import java.util.Set;

public class FirstOffsetInterceptor<K, V> extends TypePreservingInterceptor<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(FirstOffsetInterceptor.class);
    private Set<TopicUriPartition> seenPartitions = new HashSet<>();

    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
        TopicUriPartition topicUriPartition = message.getMessageId().getTopicUriPartition();
        if (!seenPartitions.contains(topicUriPartition)) {
            logger.info("First consumed offset of {}: {}", topicUriPartition, message.getMessageId().getOffset());
            seenPartitions.add(topicUriPartition);
        }
        return super.onConsume(message);
    }
}
