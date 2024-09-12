package com.pinterest.psc.consumer;

import com.pinterest.psc.common.CloseableIterator;
import com.pinterest.psc.common.TopicUriPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class PscConsumerPollMessageIterator<K, V> implements
        CloseableIterator<PscConsumerMessage<K, V>> {
    public PscConsumerPollMessageIterator() {
    }

    public static <K, V> PscConsumerPollMessageIterator<K, V> emptyIterator() {
        return new PscConsumerPollMessageIterator<K, V>() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public PscConsumerMessage<K, V> next() {
                return null;
            }

            @Override
            public Set<TopicUriPartition> getTopicUriPartitions() {
                return Collections.emptySet();
            }

            @Override
            public PscConsumerPollMessageIterator<K, V> iteratorFor(TopicUriPartition topicUriPartition) {
                return null;
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    public List<PscConsumerMessage<K, V>> asList() {
        List<PscConsumerMessage<K, V>> list = new ArrayList<>();
        while (hasNext()) {
            list.add(next());
        }
        return list;
    }

    /**
     * This API can be used to access the portion of this message iterator that is associated with the given topic
     * URI partition. Note that traversing the sub-iterator does not impact this (main) iterator.
     *
     * @param topicUriPartition the topic URI partition to filter messages on
     * @return a message iterator with only messages from the given topic URI partition.
     */
    public abstract PscConsumerPollMessageIterator<K, V> iteratorFor(TopicUriPartition topicUriPartition);

    /**
     * This API can be used to inspect all topic URI partitions associated with the returned message iterator.
     * @return a set of topic URI partitions from which messages exist in this message iterator.
     */
    public abstract Set<TopicUriPartition> getTopicUriPartitions();
}
