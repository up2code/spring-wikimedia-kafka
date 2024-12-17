package dev.up2up.springwikimediakafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final RestHighLevelClient openSearchClient;

    public KafkaConsumerService(RestHighLevelClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    @KafkaListener(topics = "wikimedia.recentchange", groupId = "wiki-kafka-group")
    public void consumeMessage(ConsumerRecord<String, String> record) {

        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();

        String id = topic + "_" + partition + "_" + offset;

        String message = record.value();
        System.out.println("Received message: " + message);

        // Create an OpenSearch index request
        IndexRequest indexRequest = new IndexRequest("wikimedia")
                .id(id) // Unique document ID
                .source("message", message); // Replace with your desired document structure

        try {
            IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
            log.info(indexResponse.getId());
        } catch (IOException e) {
            log.error("Error indexing message: " + e.getMessage());
        }
    }
}
