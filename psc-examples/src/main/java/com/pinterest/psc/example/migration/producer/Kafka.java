package com.pinterest.psc.example.migration.producer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;


public class Kafka {
        static public void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "test_producer");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);

        long current = System.currentTimeMillis();
        long start = current;
        AtomicInteger count = new AtomicInteger(0);
        while (current - start < 20_000) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("my_test_topic", ("hello world " + count.getAndIncrement()).getBytes());
            producer.send(record);
            current = System.currentTimeMillis();
        }
        System.out.printf("Produced %d records in %d ms.%n", count.get(), current - start);
        producer.close();
    }
}
