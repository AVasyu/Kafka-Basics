package org.example.kafka.tutorial2;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static JsonParser jsonParser = new JsonParser();

    public static RestHighLevelClient createClient() {
        String hostName = "avasyu-cluster-1578962741.ap-southeast-2.bonsaisearch.net";
        String userName = "8aynhb4nkq";
        String password = "8hbrjypm2y";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter-elastic-search");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // maximum number of records pulled
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("twitter_tweets"));
        return kafkaConsumer;
    }

    private static String extractIdFromTweet(String tweetJson){
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        String jsonString = "{\"foo\":\"bar\"}";

        KafkaConsumer<String, String> kafkaConsumer = createConsumer();
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            logger.info("Received Messages :- " + records.count());

            // Implement Batching Here
            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record : records){
                // 2 strategies for creating ids

                // Kafka generic Id
                /* String id = record.topic() + "_" + record.partition() + "_" + record.offset(); */

                // Twitter Feed Specific Id
                try {
                    String id = extractIdFromTweet(record.value());

                    // Insert Data Into ES
                    jsonString = record.value();
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets"
                            , id) // To make our consumer Idempotent, we provide a id such that duplicate msgs will have same id and thus will not be inserted again in ES
                            .source(jsonString, XContentType.JSON);

                    // Preparing batches here
                    bulkRequest.add(indexRequest);
                } catch(Exception e){
                    logger.error("Skipping Bad Record:- " + record.value());
                }

//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                String elasticSearchId = indexResponse.getId();
//                logger.info("ID:- " + elasticSearchId);
//                Thread.sleep(1000);
            }

            if(records.count() > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets");
                kafkaConsumer.commitSync();
                logger.info("Committed offsets");
                Thread.sleep(1000);
            }
        }

        //client.close();

//        Use Bonsai ES (avasyug16@gmail.com / System@123)
//        To create an index called twitter - PUT /twitter
//        Get Documents - /twitter/tweets/pSzliXIBpGZ1RJF2Xu0I
    }
}
