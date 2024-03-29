package com.learn.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown {
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

        //creating kafka consumer object
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(props);
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("detected a shupdown");
            kafkaConsumer.wakeup();

            try{
                mainThread.join();
            } catch (Exception e){
                e.printStackTrace();
            }
        }));

        try{
            //kafka consumer subscribing to the topic
            kafkaConsumer.subscribe(Arrays.asList(topic));

            while(true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);

                for(ConsumerRecord<String, String> record : consumerRecords){
                    log.info("key :: "+record.key()+"    value :: "+record.value()+"    partition :: "+record.partition()+"    offset :: "+record.offset());
                }
            }
        } catch (WakeupException e){
            log.error("WakeupException :: "+e);
        } catch (Exception e){
            log.error("Unexpected exception "+e);
        } finally {
            kafkaConsumer.close();
            log.info("Consumer is closed");
        }

    }
}
