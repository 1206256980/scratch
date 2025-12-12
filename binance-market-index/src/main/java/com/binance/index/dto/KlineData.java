package com.binance.index.dto;

import java.time.LocalDateTime;

/**
 * K线数据DTO
 */
public class KlineData {
    private String symbol;
    private LocalDateTime timestamp;
    private Double openPrice;
    private Double closePrice;
    private Double volume; // 交易量(USDT)

    public KlineData() {}

    public KlineData(String symbol, LocalDateTime timestamp, Double openPrice, Double closePrice, Double volume) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.openPrice = openPrice;
        this.closePrice = closePrice;
        this.volume = volume;
    }

    // Getters and Setters
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public Double getOpenPrice() {
        return openPrice;
    }

    public void setOpenPrice(Double openPrice) {
        this.openPrice = openPrice;
    }

    public Double getClosePrice() {
        return closePrice;
    }

    public void setClosePrice(Double closePrice) {
        this.closePrice = closePrice;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }
}
