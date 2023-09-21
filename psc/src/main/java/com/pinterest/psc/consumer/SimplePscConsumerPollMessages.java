package com.pinterest.psc.consumer;

import com.pinterest.psc.common.TopicUriPartition;

import java.io.IOException;
import java.util.Set;

public class SimplePscConsumerPollMessages<K, V> extends PscConsumerPollMessageIterator<K, V> {
    private final PscConsumerPollMessageIterator<K, V> pscConsumerPollMessageIterator;

    public SimplePscConsumerPollMessages(
            PscConsumerPollMessageIterator<K, V> pscConsumerPollMessageIterator
    ) {
        super();
        this.pscConsumerPollMessageIterator = pscConsumerPollMessageIterator;
    }

    @Override
    public boolean hasNext() {
        return pscConsumerPollMessageIterator.hasNext();
    }

    public PscConsumerMessage<K, V> next() {
        return pscConsumerPollMessageIterator.next();
    }

    @Override
    public PscConsumerPollMessageIterator<K, V> iteratorFor(TopicUriPartition topicUriPartition) {
        return pscConsumerPollMessageIterator.iteratorFor(topicUriPartition);
    }

    @Override
    public Set<TopicUriPartition> getTopicUriPartitions() {
        return pscConsumerPollMessageIterator.getTopicUriPartitions();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object that) {
        if (that == null)
            return false;
        if (that instanceof PscConsumerPollMessageIterator)
            return this.equals((SimplePscConsumerPollMessages<K, V>) that);
        return false;
    }

    public boolean equals(SimplePscConsumerPollMessages<K, V> that) {
        if (this == that)
            return true;
        return this.pscConsumerPollMessageIterator.equals(that.pscConsumerPollMessageIterator);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SimplePscConsumerPollMessages(");

        sb.append("records:");
        if (this.pscConsumerPollMessageIterator == null) {
            sb.append("null");
        } else {
            sb.append(this.pscConsumerPollMessageIterator);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        pscConsumerPollMessageIterator.close();
    }
}
