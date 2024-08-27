package com.tr.candlesticks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tr.candlesticks.models.Candlestick;
import com.tr.candlesticks.models.Quote;
import com.tr.candlesticks.models.QuoteEvent;
import com.tr.candlesticks.models.WindowData;
import com.tr.candlesticks.services.Consumer1;
import com.tr.candlesticks.services.WebSocketClientService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;


@SpringBootApplication
public class CandlesticksApplication implements CommandLineRunner {

	@Autowired
	WebSocketClientService consumer;

	public static void main(String[] args) {
		SpringApplication.run(CandlesticksApplication.class, args);
		ObjectMapper objectMapper = new ObjectMapper();
		AtomicReference<LocalDateTime> openTimeRef = new AtomicReference<>(LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES).plusMinutes(1));
		AtomicReference<LocalDateTime> closeTimeRef = new AtomicReference<>(openTimeRef.get().plusMinutes(1));


		WebSocketClient client = new ReactorNettyWebSocketClient();
		client.execute(URI.create("ws://localhost:8032/quotes"), session -> {
			System.out.println("WebSocket client connected");

			// Send a test message
			return session.send(Flux.just(session.textMessage("Hello from client")))
					.thenMany(session.receive().map(WebSocketMessage::getPayloadAsText)
							.timestamp()
							.window(Duration.ofMinutes(1))
							.flatMap(window -> window.collectList())
							.doOnNext(messages -> {
								LocalDateTime windowStart = getRoundedStartTime();
								LocalDateTime windowEnd = windowStart.plusMinutes(1);
								processWindow(messages, windowStart, windowEnd);
									}

							))
					.then();
		}).block();
	}

	private static LocalDateTime getRoundedStartTime() {
		LocalDateTime now = LocalDateTime.now();
		return now.withSecond(0).withNano(0).minusMinutes(now.getMinute() % 1);
	}

	private static void processWindow(List<Tuple2<Long,String>> messages, LocalDateTime windowStart, LocalDateTime windowEnd) {
		System.out.println("Processing window from " + windowStart + " to " + windowEnd);
		ObjectMapper objectMapper = new ObjectMapper();
		List<Candlestick> list = new ArrayList<>();
		messages.forEach(mesage -> {
			QuoteEvent event = null;
			try {
				event = objectMapper.readValue(mesage.getT2(), QuoteEvent.class);
				Candlestick candlestick = new Candlestick(event.data().isin(),event.data().price());
				list.add(candlestick);
				System.out.println(candlestick);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}


		});
				WindowData windowData = new WindowData(windowStart, windowEnd, list);

		System.out.println(windowData);


		// Prepare for the next window
		// Adjust openTime and closeTime for the next window if necessary

	}

	private static Candlestick  appendMessage(String message)  {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			QuoteEvent event =  objectMapper.readValue(message, QuoteEvent.class);
			Candlestick candlestick = new Candlestick(event.data().isin(),event.data().price());
			return candlestick;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;

	}

	@Override
	public void run(String... args) throws Exception {

		//consumer.connect();

	}
}
