 package com.ldi;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/*
Module 46 Java producer callback + module 47 with Keys
Prerequisite: use a topic with several partitions
 kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic demo_java --partitions 3
 */

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Kafka producer demo with callback module 46");

        //create Kafka producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //create the producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400"); // just fior showing sticky partition do not keep default setting in prod

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int j = 0; j <2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;

                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("Key : " + key + " - Partition: " + metadata.partition() + "\n"  );
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            Thread.sleep(500);
            }
        }

        //flush and close the producer: tell the producer to send all data and block until done --synchronous
        producer.flush(); //useless if you use producer.close() flush is doing by close()
        producer.close();
    }
}