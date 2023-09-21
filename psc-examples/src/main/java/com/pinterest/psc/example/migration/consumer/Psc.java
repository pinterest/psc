package com.pinterest.psc.example.migration.consumer;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.PscConsumerPollMessageIterator;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.WakeupException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.serde.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class Psc {
    static public void main(String[] args) throws ConsumerException, ConfigurationException, WakeupException {
        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test_consumer");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test_group");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, ByteArrayDeserializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, ByteArrayDeserializer.class.getName());
        PscConsumer<byte[], byte[]> consumer = new PscConsumer<>(pscConfiguration);

        consumer.subscribe(Collections.singleton("plaintext:/rn:kafka:dev:local-cloud_local-region::local-cluster:my_test_topic"));

        long current = System.currentTimeMillis();
        long start = current;
        AtomicInteger count = new AtomicInteger(0);
        while (current - start < 20_000) {
            PscConsumerPollMessageIterator<byte[], byte[]> messages = consumer.poll(Duration.ofMillis(500));
            messages.forEachRemaining(message -> count.incrementAndGet());
            current = System.currentTimeMillis();
        }
        System.out.printf("Consumed %d records in %d ms.%n", count.get(), current - start);
        consumer.close();
    }
}
