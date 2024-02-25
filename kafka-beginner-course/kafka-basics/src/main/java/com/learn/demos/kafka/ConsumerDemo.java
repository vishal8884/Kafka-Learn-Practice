package com.learn.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);


    public static void main(String[] args) {
        Properties props = new Properties();
        String consumerGroupId = "My-Consumer-1";
        String topic = "TopicA";

        props.put("bootstrap.servers", "hopeful-ghost-10378-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"aG9wZWZ1bC1naG9zdC0xMDM3OCQM0wbF8dRbRIfKefZT55xl74GaH1segy52ehg\" password=\"NzE3NTcyYTMtZGVlYS00NWE5LWI3NzMtM2E4ZjQ1MGYyZmU2\";");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", consumerGroupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        //creating kafka consumer object
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(props);


        //kafka consumer subscribing to the topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);

            for(ConsumerRecord<String, String> record : consumerRecords){
                log.info("key :: "+record.key()+"    value :: "+record.value()+"    partition :: "+record.partition()+"    offset :: "+record.offset());
            }
        }

    }
}
