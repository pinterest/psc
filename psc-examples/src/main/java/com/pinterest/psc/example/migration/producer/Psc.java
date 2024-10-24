package com.pinterest.psc.example.migration.producer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.producer.PscProducer;
import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.ByteArraySerializer;

public class Psc {
    
    static public void main(String[] args) throws ConfigurationException, ProducerException, IOException {
        String topicUri = "plaintext:/rn:kafka:dev:local-cloud_local-region::local-cluster:my_test_topic";

        PscConfiguration pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "test_producer");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, false);   // set this to false in our example since we don't need to log the client configurations
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class.getName());
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class.getName());

        PscProducer<byte[], byte[]> producer = new PscProducer<>(pscConfiguration);

        long current = System.currentTimeMillis();
        long start = current;
        AtomicInteger count = new AtomicInteger(0);
        while (current - start < 20_000) {
            PscProducerMessage<byte[], byte[]> message = new PscProducerMessage<>(topicUri, ("hello world " + count.getAndIncrement()).getBytes());
            producer.send(message);
            current = System.currentTimeMillis();
        }
        System.out.printf("Produced %d records in %d ms.%n", count.get(), current - start);
        producer.close();
    }
}
