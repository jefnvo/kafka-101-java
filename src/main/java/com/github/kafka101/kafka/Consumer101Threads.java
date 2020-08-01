package com.github.kafka101.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer101Threads {
    public static void main(String[] args) {
        new Consumer101Threads().run();

    }

    private Consumer101Threads() {
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(Consumer101Threads.class);

        String bootstrapServers = "localhost:9092";
        String groupId = "myApplication101-3";
        String topic = "firstTopic";

        //latch for dealing with multiple thread
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //create consumer runnable
        logger.info("Create the consmer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(countDownLatch, bootstrapServers,groupId,topic);

        //start thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();


        //Shut hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shut hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("App exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application Interrupted", e);
        } finally {
            logger.info("Application is closing.");
        }
    }

    public class ConsumerRunnable implements  Runnable {
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private  KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;

        public ConsumerRunnable(CountDownLatch latch,
                              String bootstrapServers,
                              String groupId,
                              String topic) {
            Properties prop = new Properties();
            prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<String, String>(prop);
            this.consumer.subscribe(Collections.singleton(topic));

            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                while(true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : consumerRecords){
                        logger.info("Key: "+ record.key() + record.value());
                        logger.info("Partition: "+ record.partition() + ", Offset: "+  record.offset());

                    }
                }
            } catch (WakeupException w) {
                logger.info("receive shut signal!");
            } finally {
                consumer.close();
                //tells  our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //interrupt poll
            consumer.wakeup();
        }
    }
}
