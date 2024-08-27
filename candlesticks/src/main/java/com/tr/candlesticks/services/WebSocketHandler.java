/*
package com.tr.candlesticks.services;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Service
public class WebSocketHandler {

    private final ReactorNettyWebSocketClient webSocketClient = new ReactorNettyWebSocketClient();

    public void start() {
        webSocketClient.execute(
                URI.create("ws://localhost:8032/quotes"),
                this::handleSession
        ).subscribe();
    }



    public Mono<Void> handleSession(WebSocketSession session) {
        return session.receive()
                .flatMap(message -> {
                    if (message.getType() == WebSocketMessage.Type.BINARY) {
                        return handleBinaryMessage(message.getPayload());
                    } else {
                        return Mono.empty(); // Handle other types if needed
                    }
                })
                .then(); // Complete after all messages are handled
    }

    private Mono<Void> handleBinaryMessage(Flux<DataBuffer> dataBufferFlux) {
        return dataBufferFlux
                .concatMap(this::processDataBuffer) // Process each DataBuffer
                .then(); // Complete after processing all DataBuffers
    }

    private Mono<Void> processDataBuffer(DataBuffer dataBuffer) {
        // Convert DataBuffer to ByteBuffer
        ByteBuffer byteBuffer = dataBuffer.asByteBuffer();
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);

        // Convert byte array to String
        String message = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("Received message: " + message);

        // Release the DataBuffer to avoid memory leaks
        dataBuffer.release();

        return Mono.empty(); // Complete after processing
    }
}*/
