package com.rkg.twitter;

import org.json.simple.parser.ParseException;

import java.io.IOException;

class MainClassConsumer {
    public static void main(String[] args) throws IOException, ParseException {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-35-application";
        String topic = "twitter_tweet";
        String host = "localhost";
        int port = 27017;
        String dataBase = "local";
        String collectionName = "tweet";
        ConsumerDemo consumerDemo = new ConsumerDemo(bootstrapServers,host,port,dataBase,collectionName);
        consumerDemo.retrieveDataFromProducer(groupId,topic);
    }
}
