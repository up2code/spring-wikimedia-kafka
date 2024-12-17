package dev.up2up.springwikimediakafka.service;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Service
public class WikimediaStreamService {

    private final WebClient webClient;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaStreamService(WebClient.Builder webClientBuilder, KafkaTemplate<String, String> kafkaTemplate) {
        this.webClient = webClientBuilder.baseUrl("https://stream.wikimedia.org").build();
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void streamRecentChanges() {

        Flux<String> stream = webClient.get()
                .uri("/v2/stream/recentchange")
                .retrieve()
                .bodyToFlux(String.class);


        stream.subscribe(event -> {
            // Log the event
//            System.out.println("Received Event: " + event);

            // Optionally, parse and process the event here
            kafkaTemplate.send("wikimedia.recentchange", event);
        }, error -> {
            // Log errors
            System.err.println("Error in stream: " + error.getMessage());
        });
    }

}
