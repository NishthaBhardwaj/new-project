package com.tr.candlesticks.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tr.candlesticks.models.Quote;
import com.tr.candlesticks.models.QuoteEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

@Service
public class Consumer1 {

    private final ReactorNettyWebSocketClient webSocketClient = new ReactorNettyWebSocketClient();

    public void start() {
        webSocketClient.execute(
                URI.create("ws://localhost:8032/quotes"),
                this::handleSession
        ).subscribe();
    }

    private Mono<Void> handleSession(WebSocketSession session) {
        // Listen to the WebSocket stream
        Flux<WebSocketMessage> messageFlux = session.receive();



        // Convert WebSocket messages to a stream of your data type
        Flux<Quote> dataFlux = messageFlux
                .map(WebSocketMessage::getPayloadAsText)
                .map(this::convertToYourDataType); // Convert JSON/String to your data type

        // Window the data into 1-minute intervals
        dataFlux
                .window(Duration.ofMinutes(1))
                .flatMap(this::processWindowedData)
                .subscribe();

        // Optionally send messages or handle more complex logic
        return Mono.never();
    }

    private Quote convertToYourDataType(String json) {
        try {
            // Create ObjectMapper instance
            ObjectMapper objectMapper = new ObjectMapper();

            // Convert JSON string to Quote record
            QuoteEvent quoteEvent = objectMapper.readValue(json, QuoteEvent.class);

            // Output the converted Quote
            System.out.println("ISIN: " + quoteEvent.data().isin());
            System.out.println("Price: " + quoteEvent.data().price());
            return quoteEvent.data();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Convert the JSON string to your data type, e.g., using Jackson or Gson
        return null; // Replace with actual conversion logic
    }

    private Flux<Void> processWindowedData(Flux<Quote> windowedFlux) {
        return windowedFlux
                .collectList()
                .doOnNext(this::aggregateData) // Process aggregated data
                .thenMany(Flux.empty());
    }

    private void aggregateData(List<Quote> dataList) {
        // Perform your aggregation logic here
        System.out.println("Processing 1-minute window with " + dataList.size() + " items");
    }

    private void processByteBuffer(ByteBuffer buffer) {
        // Convert ByteBuffer to byte array
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        // Convert byte array to String (assuming UTF-8 encoding)
        String message = new String(bytes, StandardCharsets.UTF_8);

        // Print the received message
        System.out.println("Received binary message: " + message);
    }
}