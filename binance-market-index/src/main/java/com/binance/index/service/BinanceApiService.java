package com.binance.index.service;

import com.binance.index.dto.KlineData;
import com.binance.index.dto.TickerData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class BinanceApiService {

    private static final Logger log = LoggerFactory.getLogger(BinanceApiService.class);

    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    @Value("${binance.api.base-url}")
    private String baseUrl;

    @Value("${binance.api.request-interval-ms}")
    private int requestIntervalMs;

    @Value("${index.exclude-symbols}")
    private String excludeSymbolsConfig;

    private Set<String> excludeSymbols;

    public BinanceApiService(OkHttpClient httpClient) {
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
    }

    private Set<String> getExcludeSymbols() {
        if (excludeSymbols == null) {
            excludeSymbols = Arrays.stream(excludeSymbolsConfig.split(","))
                    .map(String::trim)
                    .collect(Collectors.toSet());
        }
        return excludeSymbols;
    }

    /**
     * 获取所有U本位合约交易对（排除配置的币种）
     */
    public List<String> getAllUsdtSymbols() {
        List<String> symbols = new ArrayList<>();
        try {
            String url = baseUrl + "/fapi/v1/exchangeInfo";
            Request request = new Request.Builder().url(url).get().build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (response.isSuccessful() && response.body() != null) {
                    JsonNode root = objectMapper.readTree(response.body().string());
                    JsonNode symbolsNode = root.get("symbols");

                    if (symbolsNode != null && symbolsNode.isArray()) {
                        for (JsonNode symbolNode : symbolsNode) {
                            String symbol = symbolNode.get("symbol").asText();
                            String status = symbolNode.get("status").asText();
                            String marginAsset = symbolNode.get("marginAsset").asText();

                            // 只要USDT保证金的、交易中的合约，且不在排除列表中
                            if ("TRADING".equals(status) 
                                    && "USDT".equals(marginAsset)
                                    && !getExcludeSymbols().contains(symbol)) {
                                symbols.add(symbol);
                            }
                        }
                    }
                }
            }
            log.info("获取到 {} 个U本位交易对（已排除 {}）", symbols.size(), getExcludeSymbols());
        } catch (Exception e) {
            log.error("获取交易对列表失败: {}", e.getMessage());
        }
        return symbols;
    }

    /**
     * 获取所有交易对的24小时行情
     */
    public List<TickerData> getAll24hTickers() {
        List<TickerData> tickers = new ArrayList<>();
        try {
            String url = baseUrl + "/fapi/v1/ticker/24hr";
            Request request = new Request.Builder().url(url).get().build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (response.isSuccessful() && response.body() != null) {
                    JsonNode root = objectMapper.readTree(response.body().string());

                    if (root.isArray()) {
                        for (JsonNode tickerNode : root) {
                            String symbol = tickerNode.get("symbol").asText();
                            
                            // 跳过排除的币种
                            if (getExcludeSymbols().contains(symbol)) {
                                continue;
                            }

                            TickerData ticker = new TickerData();
                            ticker.setSymbol(symbol);
                            ticker.setLastPrice(tickerNode.get("lastPrice").asDouble());
                            ticker.setPriceChangePercent(tickerNode.get("priceChangePercent").asDouble());
                            ticker.setQuoteVolume(tickerNode.get("quoteVolume").asDouble());
                            tickers.add(ticker);
                        }
                    }
                }
            }
            log.debug("获取到 {} 个交易对的24h行情", tickers.size());
        } catch (Exception e) {
            log.error("获取24h行情失败: {}", e.getMessage());
        }
        return tickers;
    }

    /**
     * 获取单个交易对的K线数据
     * @param symbol 交易对
     * @param interval K线间隔 (5m, 15m, 1h等)
     * @param startTime 开始时间戳(毫秒)
     * @param endTime 结束时间戳(毫秒)
     * @param limit 数量限制
     */
    public List<KlineData> getKlines(String symbol, String interval, long startTime, long endTime, int limit) {
        List<KlineData> klines = new ArrayList<>();
        try {
            String url = String.format("%s/fapi/v1/klines?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=%d",
                    baseUrl, symbol, interval, startTime, endTime, limit);
            
            Request request = new Request.Builder().url(url).get().build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (response.isSuccessful() && response.body() != null) {
                    JsonNode root = objectMapper.readTree(response.body().string());

                    if (root.isArray()) {
                        for (JsonNode klineNode : root) {
                            // K线数据格式: [openTime, open, high, low, close, volume, closeTime, quoteVolume, ...]
                            long openTime = klineNode.get(0).asLong();
                            double openPrice = klineNode.get(1).asDouble();
                            double closePrice = klineNode.get(4).asDouble();
                            double quoteVolume = klineNode.get(7).asDouble();

                            LocalDateTime timestamp = LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(openTime), ZoneId.systemDefault());

                            KlineData kline = new KlineData(symbol, timestamp, openPrice, closePrice, quoteVolume);
                            klines.add(kline);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("获取K线失败 {}: {}", symbol, e.getMessage());
        }
        return klines;
    }

    /**
     * 获取多个时间段的K线（分批请求）
     */
    public List<KlineData> getKlinesWithPagination(String symbol, String interval, long startTime, long endTime, int batchLimit) {
        List<KlineData> allKlines = new ArrayList<>();
        long currentStart = startTime;

        while (currentStart < endTime) {
            List<KlineData> batch = getKlines(symbol, interval, currentStart, endTime, batchLimit);
            if (batch.isEmpty()) {
                break;
            }
            allKlines.addAll(batch);

            // 下一批从最后一条的时间之后开始
            KlineData lastKline = batch.get(batch.size() - 1);
            long lastTime = lastKline.getTimestamp()
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
            
            // 5分钟 = 300000毫秒
            currentStart = lastTime + 300000;

            // 请求间隔，避免限流
            try {
                Thread.sleep(requestIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        return allKlines;
    }

    public int getRequestIntervalMs() {
        return requestIntervalMs;
    }
}
