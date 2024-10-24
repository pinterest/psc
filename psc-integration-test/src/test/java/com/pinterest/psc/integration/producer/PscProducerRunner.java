package com.pinterest.psc.integration.producer;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import io.vavr.control.Either;

import java.io.IOException;
import java.util.concurrent.Future;

public class PscProducerRunner implements Runnable {
    private final PscConfiguration pscConfiguration;
    private String topicUriStr;
    private TopicUriPartition topicUriPartition;
    private boolean syncSend = false;
    private boolean shouldStop = false;
    private int count = 0;
    private Thread thread;
    private Exception exception;
    private PscProducer<String, String> pscProducer;

    public PscProducerRunner(PscConfiguration pscConfiguration, Either<String, TopicUriPartition> topicOrPartition, boolean syncSend) {
        this.pscConfiguration = pscConfiguration;
        this.syncSend = syncSend;

        if (topicOrPartition.isLeft())
            this.topicUriStr = topicOrPartition.getLeft();
        else
            this.topicUriPartition = topicOrPartition.get();
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

    public int getProducedMessageCount() {
        return count;
    }
    
    public void wakeupProducer() {
        this.shouldStop = true;
    }

    @Override
    public void run() {
        this.exception = null;
        if (topicUriStr == null && topicUriPartition == null)
            throw new RuntimeException("No topic/partition were provided to send messages to by PSC producer.");

        try {
            pscProducer = new PscProducer<>(pscConfiguration);
            String topicUri = topicUriStr == null ? topicUriPartition.getTopicUriAsString() : topicUriStr;
            int partition = topicUriStr == null ? topicUriPartition.getPartition() : PscUtils.NO_PARTITION;
            String kv;
            
            while (!shouldStop) {
                kv = "" + count;
                PscProducerMessage<String, String> pscProducerMessage = new PscProducerMessage<>(
                        topicUri,
                        partition,
                        kv,
                        kv
                );
                Future<MessageId> future = pscProducer.send(pscProducerMessage);
                if (syncSend)
                    future.get();
                ++count;
            }
        } catch (Exception e) {
            this.exception = e;
            e.printStackTrace();
        } finally {
            try {
                pscProducer.close();
            } catch (IOException e) {
                if (exception == null)
                    exception = e;
            }
        }
    }
}
