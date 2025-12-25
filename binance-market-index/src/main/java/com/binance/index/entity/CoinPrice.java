package com.binance.index.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * 币种价格历史记录
 * 用于存储每个币种在每个时间点的OHLC价格和成交额，支持快速计算涨幅分布
 */
@Entity
@Table(name = "coin_price", indexes = {
        @Index(name = "idx_coin_price_symbol_ts", columnList = "symbol, timestamp"),
        @Index(name = "idx_coin_price_timestamp", columnList = "timestamp"),
        // 用于时间范围查询的复合索引（timestamp在前，优化范围扫描）
        @Index(name = "idx_coin_price_ts_symbol", columnList = "timestamp, symbol")
})
public class CoinPrice {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 20)
    private String symbol;  // 交易对，如 SOLUSDT

    @Column(nullable = false)
    private LocalDateTime timestamp;  // 时间点

    @Column
    private Double openPrice;  // 开盘价

    @Column
    private Double highPrice;  // 最高价

    @Column
    private Double lowPrice;   // 最低价

    @Column(nullable = false)
    private Double price;  // 收盘价（保持向后兼容）

    @Column
    private Double volume;  // 成交额（USDT）

    public CoinPrice() {}

    // 兼容旧代码的构造函数
    public CoinPrice(String symbol, LocalDateTime timestamp, Double price) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.price = price;
    }

    // OHLC构造函数（兼容旧代码，无 volume）
    public CoinPrice(String symbol, LocalDateTime timestamp, Double openPrice, 
                     Double highPrice, Double lowPrice, Double closePrice) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.openPrice = openPrice;
        this.highPrice = highPrice;
        this.lowPrice = lowPrice;
        this.price = closePrice;
    }

    // 完整OHLCV构造函数（包含成交额）
    public CoinPrice(String symbol, LocalDateTime timestamp, Double openPrice, 
                     Double highPrice, Double lowPrice, Double closePrice, Double volume) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.openPrice = openPrice;
        this.highPrice = highPrice;
        this.lowPrice = lowPrice;
        this.price = closePrice;
        this.volume = volume;
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

    public Double getOpenPrice() {
        return openPrice;
    }

    public void setOpenPrice(Double openPrice) {
        this.openPrice = openPrice;
    }

    public Double getHighPrice() {
        return highPrice;
    }

    public void setHighPrice(Double highPrice) {
        this.highPrice = highPrice;
    }

    public Double getLowPrice() {
        return lowPrice;
    }

    public void setLowPrice(Double lowPrice) {
        this.lowPrice = lowPrice;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }
}
