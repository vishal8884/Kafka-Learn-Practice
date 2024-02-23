package com.learn.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        //create producer properties
        Properties props = new Properties();

        props.put("bootstrap.servers", "https://hopeful-ghost-10378-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"aG9wZWZ1bC1naG9zdC0xMDM3OCQM0wbF8dRbRIfKefZT55xl74GaH1segy52ehg\" password=\"NzE3NTcyYTMtZGVlYS00NWE5LWI3NzMtM2E4ZjQ1MGYyZmU2\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("batch.size","400");

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        for(int j=0;j<10;j++){
            for(int i=0;i<30;i++){
                //create a producer record
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("TopicA","Hello Kafka "+i+"   batch :: "+j);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(null == exception){
                            log.info("metadata :: "+metadata.topic()+'\n' +
                                    "partition :: "+metadata.partition()+'\n'+
                                    "offset :: "+metadata.offset()+'\n'+
                                    "key :: "+metadata.serializedKeySize()+'\n'+
                                    "time stamp:: "+metadata.timestamp());
                        }
                        else{
                            log.error("Exception occured while producer sending the data :: "+exception);
                        }
                    }
                });
            }

            //after each batch is completed sleep for 500 mills
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //flush and close producer
        producer.flush();
        producer.close();
    }
}
