package com.github.kafka101.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerCallback101 {

    public static final String KAFKA_SERVER = "localhost:9092";

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerCallback101.class);

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i<10; i++){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("firstTopic", "Hello World "+i);

            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Receive new metadata: \n"+
                                "Topic:" + recordMetadata.topic() +"\n"+
                                "Partition:"+ recordMetadata.partition() +"\n"+
                                "Offset:"+ recordMetadata.offset() + "\n"+
                                "Timestamp:"+ recordMetadata.timestamp());


                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        kafkaProducer.flush();
        kafkaProducer.close();


    }
}
