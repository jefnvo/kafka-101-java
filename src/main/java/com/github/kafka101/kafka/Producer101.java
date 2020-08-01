package com.github.kafka101.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class Producer101 {

    public static final String KAFKA_SERVER = "localhost:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("firstTopic", "Hello world");

        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
