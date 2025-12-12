package com.binance.index.dto;

/**
 * 24小时行情数据DTO
 */
public class TickerData {
    private String symbol;
    private Double lastPrice;
    private Double priceChangePercent;
    private Double quoteVolume; // 交易量(USDT)

    public TickerData() {}

    public TickerData(String symbol, Double lastPrice, Double priceChangePercent, Double quoteVolume) {
        this.symbol = symbol;
        this.lastPrice = lastPrice;
        this.priceChangePercent = priceChangePercent;
        this.quoteVolume = quoteVolume;
    }

    // Getters and Setters
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Double getLastPrice() {
        return lastPrice;
    }

    public void setLastPrice(Double lastPrice) {
        this.lastPrice = lastPrice;
    }

    public Double getPriceChangePercent() {
        return priceChangePercent;
    }

    public void setPriceChangePercent(Double priceChangePercent) {
        this.priceChangePercent = priceChangePercent;
    }

    public Double getQuoteVolume() {
        return quoteVolume;
    }

    public void setQuoteVolume(Double quoteVolume) {
        this.quoteVolume = quoteVolume;
    }
}
