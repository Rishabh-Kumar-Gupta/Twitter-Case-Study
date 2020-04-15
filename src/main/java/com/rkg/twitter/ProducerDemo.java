package com.rkg.twitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    Properties properties;
    KafkaProducer<String, String> producer;
    ProducerRecord<String,String> record;
    public ProducerDemo(String bootstrapServers){
        this.properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(properties);
    }

    public void sendValue(String topic, String value){
        ProducerRecord<String,String> record = new ProducerRecord<>(topic, value);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
