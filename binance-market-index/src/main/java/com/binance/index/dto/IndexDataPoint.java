package com.binance.index.dto;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * API响应DTO - 单个指数数据点
 */
public class IndexDataPoint {
    private Long timestamp; // 毫秒时间戳 (UTC)
    private Double indexValue;
    private Double totalVolume;
    private Integer coinCount;
    private Integer upCount; // 上涨币种数
    private Integer downCount; // 下跌币种数
    private Double adr; // 涨跌比率

    public IndexDataPoint() {
    }

    public IndexDataPoint(LocalDateTime time, Double indexValue, Double totalVolume, Integer coinCount) {
        this.timestamp = time.toInstant(ZoneOffset.UTC).toEpochMilli();
        this.indexValue = indexValue;
        this.totalVolume = totalVolume;
        this.coinCount = coinCount;
    }

    public IndexDataPoint(LocalDateTime time, Double indexValue, Double totalVolume,
            Integer coinCount, Integer upCount, Integer downCount, Double adr) {
        this.timestamp = time.toInstant(ZoneOffset.UTC).toEpochMilli();
        this.indexValue = indexValue;
        this.totalVolume = totalVolume;
        this.coinCount = coinCount;
        this.upCount = upCount;
        this.downCount = downCount;
        this.adr = adr;
    }

    // Getters and Setters
    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
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
