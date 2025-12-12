package com.binance.index.dto;

import java.time.LocalDateTime;

/**
 * API响应DTO - 单个指数数据点
 */
public class IndexDataPoint {
    private LocalDateTime timestamp;
    private Double indexValue;
    private Double totalVolume;
    private Integer coinCount;

    public IndexDataPoint() {}

    public IndexDataPoint(LocalDateTime timestamp, Double indexValue, Double totalVolume, Integer coinCount) {
        this.timestamp = timestamp;
        this.indexValue = indexValue;
        this.totalVolume = totalVolume;
        this.coinCount = coinCount;
    }

    // Getters and Setters
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
