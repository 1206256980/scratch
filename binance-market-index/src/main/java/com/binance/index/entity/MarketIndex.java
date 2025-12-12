package com.binance.index.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "market_index", indexes = {
    @Index(name = "idx_timestamp", columnList = "timestamp")
})
public class MarketIndex {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private LocalDateTime timestamp;

    @Column(nullable = false)
    private Double indexValue; // 加权平均涨跌幅%

    @Column
    private Double totalVolume; // 总交易量

    @Column
    private Integer coinCount; // 参与计算的币种数

    public MarketIndex() {}

    public MarketIndex(LocalDateTime timestamp, Double indexValue, Double totalVolume, Integer coinCount) {
        this.timestamp = timestamp;
        this.indexValue = indexValue;
        this.totalVolume = totalVolume;
        this.coinCount = coinCount;
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public Double getIndexValue() {
        return indexValue;
    }

    public void setIndexValue(Double indexValue) {
        this.indexValue = indexValue;
    }

    public Double getTotalVolume() {
        return totalVolume;
    }

    public void setTotalVolume(Double totalVolume) {
        this.totalVolume = totalVolume;
    }

    public Integer getCoinCount() {
        return coinCount;
    }

    public void setCoinCount(Integer coinCount) {
        this.coinCount = coinCount;
    }
}
