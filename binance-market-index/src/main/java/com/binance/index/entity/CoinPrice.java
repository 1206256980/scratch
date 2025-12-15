package com.binance.index.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 币种价格历史记录
 * 用于存储每个币种在每个时间点的价格，支持快速计算涨幅分布
 */
@Entity
@Table(name = "coin_price", indexes = {
        @Index(name = "idx_coin_price_symbol_ts", columnList = "symbol, timestamp"),
        @Index(name = "idx_coin_price_timestamp", columnList = "timestamp")
})
public class CoinPrice {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 20)
    private String symbol;  // 交易对，如 SOLUSDT

    @Column(nullable = false)
    private LocalDateTime timestamp;  // 时间点

    @Column(nullable = false)
    private Double price;  // 价格

    public CoinPrice() {}

    public CoinPrice(String symbol, LocalDateTime timestamp, Double price) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.price = price;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

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

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
