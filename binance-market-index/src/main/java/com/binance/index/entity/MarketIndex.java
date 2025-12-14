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

    @Column
    private Integer upCount; // 上涨币种数

    @Column
    private Integer downCount; // 下跌币种数

    @Column
    private Double adr; // 涨跌比率 (Advance Decline Ratio)

    public MarketIndex() {
    }

    public MarketIndex(LocalDateTime timestamp, Double indexValue, Double totalVolume, Integer coinCount) {
        this.timestamp = timestamp;
        this.indexValue = indexValue;
        this.totalVolume = totalVolume;
        this.coinCount = coinCount;
    }

    public MarketIndex(LocalDateTime timestamp, Double indexValue, Double totalVolume,
            Integer coinCount, Integer upCount, Integer downCount, Double adr) {
        this.timestamp = timestamp;
        this.indexValue = indexValue;
        this.totalVolume = totalVolume;
        this.coinCount = coinCount;
        this.upCount = upCount;
        this.downCount = downCount;
        this.adr = adr;
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

    public Integer getUpCount() {
        return upCount;
    }

    public void setUpCount(Integer upCount) {
        this.upCount = upCount;
    }

    public Integer getDownCount() {
        return downCount;
    }

    public void setDownCount(Integer downCount) {
        this.downCount = downCount;
    }

    public Double getAdr() {
        return adr;
    }

    public void setAdr(Double adr) {
        this.adr = adr;
    }
}
