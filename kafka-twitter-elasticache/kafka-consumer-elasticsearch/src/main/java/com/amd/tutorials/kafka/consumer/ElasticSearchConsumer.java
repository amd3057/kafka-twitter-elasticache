package com.amd.tutorials.kafka.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ElasticSearchConsumer {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException, ConfigurationException {

        new ElasticSearchConsumer().run();
    }

    private void run() throws ConfigurationException, IOException {
        String topic = "twitter_tweets";
        String groupId = "kafka-twitter-elasticsearch";
        String bootstrapServer = "127.0.0.1:9092";
        RestHighLevelClient client = createHighLevelClient();
        CountDownLatch latch = new CountDownLatch(1);

        final ConsumerThread run = new ConsumerThread(latch, bootstrapServer, groupId, topic, client);
        Thread thread = new Thread(run);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook ");
            run.shutdown();

        }));
    }

    private RestHighLevelClient createHighLevelClient() throws ConfigurationException {

        String hostname = "";
        String username = "";
        String password = "";

        if (hostname.isEmpty() || username.isEmpty() || password.isEmpty()) {
            logger.error("Please set hostname, username, password information from elastic search access information");
            throw new ConfigurationException("Please set hostname, username, password information from elastic search access information");
        }

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        return new RestHighLevelClient(builder);;
    }

    public class ConsumerThread implements Runnable {
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> kafkaConsumer;
        private final RestHighLevelClient client;

         ConsumerThread(CountDownLatch latch, String bootstrapServer, String groupId, String topic, RestHighLevelClient client) {
            this.latch = latch;
            this.client = client;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5"); //Setting records to minimum so that we will not exhaust free version limitations of elasticsearch quickly

            kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    BulkRequest bulkRequest = new BulkRequest();
                    logger.info("Received records count" + consumerRecords.count());

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Records Received :" + record.value());
                        String id = extractIdFromTweetJson(record.value());
                        if (id != null) {
                            IndexRequest indexRequest = new IndexRequest().index("twitter").id(id).source(record.value(), XContentType.JSON);
                            bulkRequest.add(indexRequest);
                        } else {
                            logger.warn("Skipping record without valid id");
                        }
                    }
                    if (consumerRecords.count() > 0) {
                        client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        kafkaConsumer.commitAsync();
                    }
                }
            } catch (WakeupException we) {
                logger.info("Expected Exception wakeup ");
                kafkaConsumer.close();

            } catch (IOException ie) {
                logger.warn("Exception while sending data to elasticsearch, retry will happen", ie);
            }

        }

        public void shutdown() {
            try {
                client.close();
            } catch (IOException e) {
                logger.error("Error while closing Elastic Search Rest client .", e);
            }
            kafkaConsumer.wakeup();
            latch.countDown();
        }

        private String extractIdFromTweetJson(String tweetJson) {
            try {
                return new Gson()
                        .fromJson(tweetJson, JsonObject.class)
                        .get("id_str")
                        .getAsString();
            } catch (JsonSyntaxException jse) {
                //we are not bothered for manually added records to topic
                return null;
            }


        }

    }
}
