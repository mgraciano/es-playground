package com.betha.kafka;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerSample {

    public static void main(String[] args) {
        final Properties properties = new Properties();

        // Kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // Producer acks
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            final LocalDateTime start = LocalDateTime.now();
            for (int i = 0; i < 100000; i++) {
                final ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "" + i, "message: " + i);
                producer.send(record);
            }
            System.out.println("Took " + Duration.between(start, LocalDateTime.now()));
        }
    }
}
