package com.github.kafka101.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.Arrays;
import java.util.Properties;

public class Consumer101AssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer101.class);

        String bootstrapServers = "localhost:9092";
        String topic = "firstTopic";

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        //Assign and Seek are mostly used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);

        consumer.assign(Arrays.asList(partitionToReadFrom));

        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessageToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while(keepOnReading) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords){
                numberOfMessagesReadSoFar+=1;
                logger.info("Key: "+ record.key() + record.value());
                logger.info("Partition: "+ record.partition() + ", Offset: "+  record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessageToRead) {
                    keepOnReading = false;
                    break;
                }

            }
        }
        logger.info("Exiting application");

    }
}
