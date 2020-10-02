package org.example.kafka.kafkaStreams;


import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        // Create Properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams"); // Similar to Consumer Group Id Prop but for kafka streams
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Create A Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Input Topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                // Filter for tweets for which users have over 10k followers
                (k, jsonTweet) -> extractFollowersCountFromTweet(jsonTweet) > 10000
        );
        filteredStream.to("important_tweets");

        // Build the Topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // Start Kafka Streams Application
        kafkaStreams.start();
    }

    public static int extractFollowersCountFromTweet(String jsonTweet) {
        int followers = 0;
        try {
            followers = jsonParser.parse(jsonTweet).getAsJsonObject().get("user").getAsJsonObject()
                    .get("followers_count").getAsInt();
        } catch (Exception e) { }
            return followers;
    }
}
