package com.tr.candlesticks.services;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Service
public class WebSocketClientService {

    private final WebSocketClient webSocketClient;

    public WebSocketClientService() {
        this.webSocketClient = new ReactorNettyWebSocketClient();
    }

    public void connect() {
        webSocketClient.execute(
                URI.create("ws://localhost:8032/quotes"),
                this::handleSession
        ).subscribe();
    }

    private Mono<Void> handleSession(WebSocketSession session) {
        // Print out when the connection is established
        System.out.println("WebSocket connection established");
         session.receive().map(data -> {
             System.out.println(data);
             return data;
         } );


        return Mono.empty();
    }
}