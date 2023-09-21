package com.pinterest.psc.discovery;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerUtils;
import com.pinterest.psc.exception.consumer.ConsumerException;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PscConsumerCreator<K, V> implements Runnable {
    private final PscConfiguration pscConfiguration;
    private final Set<String> subscriptionTopicUris;
    private Thread thread;
    private Exception exception;
    private PscConsumer<K, V> pscConsumer;
    private Map<String, String> connectionStringByTopicUriString;

    public PscConsumerCreator(PscConfiguration pscConfiguration, Set<String> subscriptionTopicUris) {
        this.pscConfiguration = pscConfiguration;
        this.subscriptionTopicUris = subscriptionTopicUris;
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

    public Map<String, String> getConnectionStringByTopicUriString() {
        return connectionStringByTopicUriString;
    }

    @Override
    public void run() {
        this.exception = null;
        try {
            pscConsumer = new PscConsumer<>(pscConfiguration);
            pscConsumer.subscribe(subscriptionTopicUris);
            connectionStringByTopicUriString = PscConsumerUtils.getBackendConsumerConfiguration(pscConsumer)
                    .entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().getString("bootstrap.servers")
                    ));
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
