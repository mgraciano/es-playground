package com.betha.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerSample {

    public static void main(String[] args) {
        final Properties properties = new Properties();

        // Kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", "bth.sample.6");

        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");

        properties.setProperty("auto.offset.reset", "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("first_topic"));

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(
                        "Partition: " + record.partition() + ",\t" +
                        "Offset: " + record.offset() + ",\t" +
                        "Key: " + record.key() + ", \t" +
                        "Value: " + record.value()
                );
            }
        }
    }
}
