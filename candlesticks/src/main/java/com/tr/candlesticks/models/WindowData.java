package com.tr.candlesticks.models;

import java.time.LocalDateTime;
import java.util.List;

public record WindowData(LocalDateTime openWindowTime, LocalDateTime closeWindowTime, List<Candlestick> data) {}
