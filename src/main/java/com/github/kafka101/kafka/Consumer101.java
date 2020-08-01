package com.github.kafka101.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.Collections;
import java.util.Properties;

public class Consumer101 {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer101.class);

        String bootstrapServers = "localhost:9092";
        String groupId = "myApplication101";
        String topic = "firstTopic";

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        consumer.subscribe(Collections.singleton(topic));

        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords){
                logger.info("Key: "+ record.key() + record.value());
                logger.info("Partition: "+ record.partition() + ", Offset: "+  record.offset());

            }
        }


    }
}
