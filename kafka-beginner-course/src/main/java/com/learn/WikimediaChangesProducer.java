package com.learn;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) {
        //create producer properties
        Properties props = new Properties();

        props.put("bootstrap.servers", "https://hopeful-ghost-10378-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"aG9wZWZ1bC1naG9zdC0xMDM3OCQM0wbF8dRbRIfKefZT55xl74GaH1segy52ehg\" password=\"NzE3NTcyYTMtZGVlYS00NWE5LWI3NzMtM2E4ZjQ1MGYyZmU2\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        System.out.println("Creating event handler");

        EventHandler eventHandler = new WikimediaChangeHandler(producer,topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler,URI.create(url));
        EventSource eventSource = builder.build();

        eventSource.start();

        //Start the producer in other thread..so that programm does not die
        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
