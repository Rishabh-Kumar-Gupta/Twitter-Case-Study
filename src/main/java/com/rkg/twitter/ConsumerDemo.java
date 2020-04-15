package com.rkg.twitter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author Rishabh Kumar Gupta
 */
public class ConsumerDemo {
    private String bootstrapServers;
    String host ;
    int port;
    String dataBase ;
    String collectionName ;
    String groupId;
    Properties properties;
    public ConsumerDemo(String bootstrapServers, String host, int port, String dataBase, String collectionName, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.host = host;
        this.port = port;
        this.dataBase = dataBase;
        this.collectionName = collectionName;
        this.groupId = groupId;
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public void retrieveDataFromProducer(String topic){
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(this.properties);
        consumer.subscribe(Collections.singletonList(topic));

        /**
         * MongoDB Connection Set-up
         */
        MongoConnectionJava mongoConnectionJava = new MongoConnectionJava(host,port,dataBase,collectionName);
        System.out.println("Connected to MongoDB Successfully");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key());
                logger.info("Value: " + record.value());
                logger.info("Partition: " + record.partition());
                logger.info("offset: " + record.offset());
                String msg = record.value();
                System.out.println(mongoConnectionJava.insertSingleData(msg));
            }
        }
    }
}







