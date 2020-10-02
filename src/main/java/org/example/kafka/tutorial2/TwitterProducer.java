//package org.example.kafka.tutorial2;
//
//import com.google.common.collect.Lists;
//import com.twitter.hbc.ClientBuilder;
//import com.twitter.hbc.core.Client;
//import com.twitter.hbc.core.Constants;
//import com.twitter.hbc.core.Hosts;
//import com.twitter.hbc.core.HttpHosts;
//import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
//import com.twitter.hbc.core.processor.StringDelimitedProcessor;
//import com.twitter.hbc.httpclient.auth.Authentication;
//import com.twitter.hbc.httpclient.auth.OAuth1;
//import org.apache.kafka.clients.producer.*;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.omg.SendingContext.RunTime;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.List;
//import java.util.Properties;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//
//public class TwitterProducer {
//
//    Logger logger = LoggerFactory.getLogger(this.getClass().getName());
//
//    String consumerKey = "hiWzx4X2qubRPVQBtzDmveF2z";
//    String consumerSecret = "w8USYaRMMpYzcmbRYntE2jKBajZRycWrZpGcSqGd8pHDVu29f8";
//    String token = "4032712400-AfeB1J0wIrq6VfCCRP7o1KlrYMSykLnsubwcB5d";
//    String tokenSecret = "9VgBQ73UPqlQ0kiLjBVUFqaPyMPXnZl7fUzgglA6xpGak";
//
//    List<String> terms = Lists.newArrayList("bitcoin");
//
//    public TwitterProducer() {}
//
//    public static void main(String[] args) {
//        new TwitterProducer().run();
//    }
//
//    public void run() {
//        // Set up blocking queue. Be sure to size these properly based on the expected TPS of our stream
//        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(5);
//
//        // Create A Twitter Client
//        Client hosebirdClient = createTwitterClient(msgQueue);
//        // Attempts to establish a connection.
//        hosebirdClient.connect();
//
//        // Create A Kafka Producer
//        KafkaProducer<String, String> producer = createKafkaProducer();
//
//        // Add a shutdown hook
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            logger.info("Stopping Application");
//            hosebirdClient.stop();
//            producer.close();
//        }));
//
//        // Loop to send tweets to Kafka
//        while (!hosebirdClient.isDone()) {
//            String msg = null;
//            try {
//                msg = msgQueue.poll(5, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//                hosebirdClient.stop();
//            }
//            if (msg != null){
//                logger.info(msg);
//                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
//                    @Override
//                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                        if (e!= null){
//                            logger.error("Something Went Wrong", e);
//                        }
//                    }
//                });
//            }
//        }
//        logger.info("End Of Application");
//
//
//    }
//
//    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
//
//        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
//        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
//        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
//        // Optional: set up some followings and track terms
//        hosebirdEndpoint.trackTerms(terms);
//
//        // These secrets should be read from a config file
//        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
//
//        // Create a Client
//        ClientBuilder builder = new ClientBuilder()
//                .name("Hosebird-Client-01")                              // optional: mainly for the logs
//                .hosts(hosebirdHosts)
//                .authentication(hosebirdAuth)
//                .endpoint(hosebirdEndpoint)
//                .processor(new StringDelimitedProcessor(msgQueue));
//
//        Client hosebirdClient = builder.build();
//        return hosebirdClient;
//    }
//
//    public KafkaProducer<String, String> createKafkaProducer(){
//        Properties properties = new Properties();
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // What type of data are we sending to kafka for serialization and deserialization of it
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        // Create A safe producer
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
//        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // As kafka 2.0 > 1.1, we can use 5 instead of 1.
//
//        // High Throughput producer (at expense of a bit of latency and CPU Usage(To form batches))
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32Kb Batch Size
//
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
//
//        return producer;
//    }
//}
