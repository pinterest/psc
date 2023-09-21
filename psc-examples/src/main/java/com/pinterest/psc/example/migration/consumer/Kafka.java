package com.pinterest.psc.example.migration.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class Kafka {
    static public void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "test_consumer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_group");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton("my_test_topic"));

        long current = System.currentTimeMillis();
        long start = current;
        AtomicInteger count = new AtomicInteger(0);
        while (current - start < 20_000) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            records.iterator().forEachRemaining(record -> count.incrementAndGet());
            current = System.currentTimeMillis();
        }
        System.out.printf("Consumed %d records in %d ms.%n", count.get(), current - start);
        consumer.close();
    }
}
