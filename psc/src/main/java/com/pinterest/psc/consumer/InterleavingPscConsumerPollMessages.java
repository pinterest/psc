package com.pinterest.psc.consumer;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.exception.ExceptionMessage;
import com.pinterest.psc.logging.PscLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

public class InterleavingPscConsumerPollMessages<K, V> extends PscConsumerPollMessageIterator<K, V> {

    private static final PscLogger logger = PscLogger.getLogger(InterleavingPscConsumerPollMessages.class);
    private final List<PscConsumerPollMessageIterator<K, V>> pscConsumerPollMessageIteratorList;
    private int nextIteratorIndex;
    private final int iteratorCount;

    public InterleavingPscConsumerPollMessages(
            List<PscConsumerPollMessageIterator<K, V>> pscConsumerPollMessageIteratorList
    ) {
        super();
        this.pscConsumerPollMessageIteratorList = pscConsumerPollMessageIteratorList;
        this.nextIteratorIndex = 0;
        this.iteratorCount = this.pscConsumerPollMessageIteratorList.size();
    }

    @Override
    public boolean hasNext() {
        for (int i = 0; i < iteratorCount; ++i) {
            if (pscConsumerPollMessageIteratorList.get(nextIteratorIndex).hasNext())
                return true;
            nextIteratorIndex = (nextIteratorIndex + 1) % iteratorCount;
        }
        return false;
    }

    @Override
    public PscConsumerMessage<K, V> next() {
        if (!hasNext())
            throw new NoSuchElementException(ExceptionMessage.ITERATOR_OUT_OF_ELEMENTS);

        PscConsumerMessage<K, V> pscConsumerMessage = pscConsumerPollMessageIteratorList.get(nextIteratorIndex).next();
        nextIteratorIndex = (nextIteratorIndex + 1) % iteratorCount;
        return pscConsumerMessage;
    }

    @Override
    public Set<TopicUriPartition> getTopicUriPartitions() {
        return pscConsumerPollMessageIteratorList.stream()
                .map(PscConsumerPollMessageIterator::getTopicUriPartitions)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public PscConsumerPollMessageIterator<K, V> iteratorFor(TopicUriPartition topicUriPartition) {
        List<PscConsumerPollMessageIterator<K, V>> perPartitionPscConsumerPollMessageIteratorList = new ArrayList<>();
        pscConsumerPollMessageIteratorList.forEach(iterator -> {
            if (iterator.iteratorFor(topicUriPartition) != null)
                perPartitionPscConsumerPollMessageIteratorList.add(iterator.iteratorFor(topicUriPartition));
        });
        return new InterleavingPscConsumerPollMessages<>(perPartitionPscConsumerPollMessageIteratorList);
    }

    @Override
    public void close() throws IOException {
        pscConsumerPollMessageIteratorList.forEach(iterator -> {
            try {
                iterator.close();
            } catch (IOException e) {
                // best effort close
                logger.warn("Failed to close backend iterator", e);
            }
        });
    }
}
