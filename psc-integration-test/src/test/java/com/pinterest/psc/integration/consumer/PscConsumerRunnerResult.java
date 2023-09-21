package com.pinterest.psc.integration.consumer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.PscConsumerMessage;

import java.util.List;
import java.util.Set;

public class PscConsumerRunnerResult<K, V> {
    Set<TopicUriPartition> assignment;
    List<PscConsumerMessage<K, V>> messages;
    Set<MessageId> committed;

    public PscConsumerRunnerResult(
            Set<TopicUriPartition> assignment,
            List<PscConsumerMessage<K, V>> messages,
            Set<MessageId> committed
    ) {
        this.assignment = assignment;
        this.messages = messages;
        this.committed = committed;
    }

    public Set<TopicUriPartition> getAssignment() {
        return assignment;
    }

    public List<PscConsumerMessage<K, V>> getMessages() {
        return messages;
    }

    public Set<MessageId> getCommitted() {
        return committed;
    }
}
