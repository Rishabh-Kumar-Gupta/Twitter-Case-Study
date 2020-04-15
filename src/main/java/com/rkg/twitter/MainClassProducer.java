package com.rkg.twitter;

import com.google.common.collect.Lists;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

public class MainClassProducer {
    public static void main(String[] args) throws IOException, ParseException {
        String consumerKey = "";
        String consumerSecret = "";
        String token = "";
        String secret = "";
        String bootstrapServers = "";
        String topic = "twitter_tweet";
        List<String> keyword =  Lists.newArrayList("corona","Corona","Covid","Virus");

        TwitterProducer twitterProducer = new TwitterProducer(consumerKey,consumerSecret,token,secret,bootstrapServers,topic,keyword);
        twitterProducer.run();
    }

}



