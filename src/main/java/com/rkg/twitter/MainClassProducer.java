package com.rkg.twitter;

import com.google.common.collect.Lists;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

public class MainClassProducer {
    public static void main(String[] args) throws IOException, ParseException {
        String consumerKey = "IW7FxxxBbC5iULPsC8vqtZUCR";
        String consumerSecret = "w7pJamZhSlFy78R9PGz9jQu1D9aurPAnala83JfavHD46De9lN";
        String token = "1247163432345800710-91ONcQJvFWdWK8UqyW1aqzHJdupFur";
        String secret = "U9wEgdnijN5fr84YXzur19oYWnEsz6d1olUB3k2niGJ3P";
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "twitter_tweet";
        List<String> keyword =  Lists.newArrayList("corona","Corona","Covid","Virus");

        TwitterProducer twitterProducer = new TwitterProducer(consumerKey,consumerSecret,token,secret,bootstrapServers,topic,keyword);
        twitterProducer.run();
    }

}



