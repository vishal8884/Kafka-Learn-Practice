package com.learn.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        //create producer properties
        Properties props = new Properties();

        props.put("bootstrap.servers", "https://hopeful-ghost-10378-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"aG9wZWZ1bC1naG9zdC0xMDM3OCQM0wbF8dRbRIfKefZT55xl74GaH1segy52ehg\" password=\"NzE3NTcyYTMtZGVlYS00NWE5LWI3NzMtM2E4ZjQ1MGYyZmU2\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        //create a producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("TopicA","My First kafka message");

        //send data
        producer.send(producerRecord);

        //flush and close producer
        producer.flush();
        producer.close();
    }
}
