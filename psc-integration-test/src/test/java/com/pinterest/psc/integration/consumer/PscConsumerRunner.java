package com.pinterest.psc.integration.consumer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.utils.PscTestUtils;
import io.vavr.control.Either;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PscConsumerRunner<K, V> implements Runnable {
    private final PscConfiguration pscConfiguration;
    private Set<String> topicUriStrs;
    private Set<TopicUriPartition> topicUriPartitions;
    private PscConsumerRunnerResult<K, V> pscConsumerRunnerResult;
    private final int pollTimeoutMs;
    private Either<Map<TopicUriPartition, Long>, Set<MessageId>> seekPosition;
    private boolean manuallyCommitMessageIds = false;
    private boolean asyncCommit = false;
    private boolean commitSelectMessageIds = false;
    private boolean seekPositionIsTimestamp = false;
    private Thread thread;
    private Exception exception;
    private PscConsumer<String, String> pscConsumer;

    public PscConsumerRunner(PscConfiguration pscConfiguration, Either<Set<String>, Set<TopicUriPartition>> topicsOrPartitions, int pollTimeoutMs) {
        this.pscConfiguration = pscConfiguration;
        this.pollTimeoutMs = pollTimeoutMs;

        if (topicsOrPartitions.isLeft())
            this.topicUriStrs = topicsOrPartitions.getLeft();
        else
            this.topicUriPartitions = topicsOrPartitions.get();
    }

    public void setMessageIdsToSeek(Set<MessageId> messageIdsToSeek) {
        seekPosition = Either.right(messageIdsToSeek);
    }

    public void setPartitionTimestampsToSeek(Map<TopicUriPartition, Long> partitionTimestampsToSeek) {
        seekPosition = Either.left(partitionTimestampsToSeek);
        seekPositionIsTimestamp = true;
    }

    public void setPartitionOffsetsToSeek(Map<TopicUriPartition, Long> partitionOffsetsToSeek) {
        seekPosition = Either.left(partitionOffsetsToSeek);
        seekPositionIsTimestamp = false;
    }

    /**
     * If set and consumer is set to auto-commit=false, it will commit a fixed number of first messages
     * from each partition.
     *
     * @param shouldCommit whether the PSC consumer should also manually commit offsets
     */
    public void setManuallyCommitMessageIds(boolean shouldCommit) {
        this.manuallyCommitMessageIds = shouldCommit;
    }

    public void setAsyncCommit(boolean asyncCommit) {
        this.asyncCommit = asyncCommit;
    }

    public void setCommitSelectMessageIds(boolean commitSelectMessageIds) {
        this.commitSelectMessageIds = commitSelectMessageIds;
    }

    public Set<TopicUriPartition> getAssignment() {
        return pscConsumerRunnerResult == null ? null : pscConsumerRunnerResult.getAssignment();
    }

    public List<PscConsumerMessage<K, V>> getMessages() {
        return pscConsumerRunnerResult == null ? null : pscConsumerRunnerResult.getMessages();
    }

    public Set<MessageId> getCommitted() {
        return pscConsumerRunnerResult == null ? null : pscConsumerRunnerResult.getCommitted();
    }

    public Exception getException() {
        return exception;
    }

    public void kickoff() {
        if (thread != null && thread.isAlive())
            throw new RuntimeException("The consumer runner is already running!");
        thread = new Thread(this);
        thread.start();
    }

    public void waitForCompletion() throws InterruptedException {
        thread.join();
    }

    public void kickoffAndWaitForCompletion() throws InterruptedException {
        kickoff();
        waitForCompletion();
    }

    public void wakeupConsumer() {
        // wait for the consumer object to be created
        while (pscConsumer == null) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        pscConsumer.wakeup();
    }

    @Override
    public void run() {
        this.exception = null;
        if (topicUriStrs == null && topicUriPartitions == null)
            throw new RuntimeException("No topics/partitions were provided for subscription/assignment of PSC consumer.");

        try {
            pscConsumer = new PscConsumer<>(pscConfiguration);
            pscConsumerRunnerResult = topicUriStrs != null ?
                    PscTestUtils.subscribeAndConsume(pscConsumer, topicUriStrs, pollTimeoutMs, seekPosition, seekPositionIsTimestamp, manuallyCommitMessageIds, asyncCommit, commitSelectMessageIds) :
                    PscTestUtils.assignAndConsume(pscConsumer, topicUriPartitions, pollTimeoutMs, seekPosition, seekPositionIsTimestamp, manuallyCommitMessageIds, asyncCommit, commitSelectMessageIds);
            seekPosition = null;
        } catch (Exception e) {
            this.exception = e;
        } finally {
            try {
                pscConsumer.close();
            } catch (ConsumerException e) {
                e.printStackTrace();
            }
        }
    }
}
