package com.pinterest.psc.consumer;

import com.pinterest.psc.common.TopicUriPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

public class PscConsumerMessagesIterable<K, V> implements Iterable<PscConsumerMessage<K, V>> {

    List<PscConsumerMessage<K, V>> messages;
    Map<TopicUriPartition, List<PscConsumerMessage<K, V>>> messagesByTopicUriPartition;

    public PscConsumerMessagesIterable(PscConsumerPollMessageIterator<K, V> iterator) {
        this.messages = iterator.asList();
        this.messagesByTopicUriPartition = new HashMap<>();
        for (PscConsumerMessage<K, V> message : messages) {
            TopicUriPartition topicUriPartition = message.getMessageId().getTopicUriPartition();
            if (!messagesByTopicUriPartition.containsKey(topicUriPartition)) {
                messagesByTopicUriPartition.put(topicUriPartition, new ArrayList<>());
            }
            messagesByTopicUriPartition.get(topicUriPartition).add(message);
        }
    }

    @Override
    public Iterator<PscConsumerMessage<K, V>> iterator() {
        return messages.iterator();
    }

    @Override
    public void forEach(Consumer<? super PscConsumerMessage<K, V>> action) {
        this.messages.forEach(action);
    }

    @Override
    public Spliterator<PscConsumerMessage<K, V>> spliterator() {
        return this.messages.spliterator();
    }

    public Set<TopicUriPartition> getTopicUriPartitions() {
        return messagesByTopicUriPartition.keySet();
    }

    public List<PscConsumerMessage<K, V>> getMessages() {
        return messages;
    }

    public List<PscConsumerMessage<K, V>> getMessagesForTopicUriPartition(TopicUriPartition topicUriPartition) {
        return messagesByTopicUriPartition.get(topicUriPartition);
    }

    public static <K, V> PscConsumerMessagesIterable<K, V> emptyIterable() {
        return new PscConsumerMessagesIterable<>(PscConsumerPollMessageIterator.emptyIterator());
    }
}
