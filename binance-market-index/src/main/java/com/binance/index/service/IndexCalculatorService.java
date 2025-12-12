package com.binance.index.service;

import com.binance.index.dto.KlineData;
import com.binance.index.dto.TickerData;
import com.binance.index.entity.MarketIndex;
import com.binance.index.repository.MarketIndexRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class IndexCalculatorService {

    private static final Logger log = LoggerFactory.getLogger(IndexCalculatorService.class);

    private final BinanceApiService binanceApiService;
    private final MarketIndexRepository marketIndexRepository;

    // 缓存各币种的基准价格（3天前的价格）
    private Map<String, Double> basePrices = new HashMap<>();
    private LocalDateTime basePriceTime;

    public IndexCalculatorService(BinanceApiService binanceApiService, 
                                   MarketIndexRepository marketIndexRepository) {
        this.binanceApiService = binanceApiService;
        this.marketIndexRepository = marketIndexRepository;
    }

    /**
     * 计算并保存当前时刻的市场指数（实时采集用）
     */
    public MarketIndex calculateAndSaveCurrentIndex() {
        List<TickerData> tickers = binanceApiService.getAll24hTickers();
        if (tickers.isEmpty()) {
            log.warn("无法获取行情数据");
            return null;
        }

        // 如果没有基准价格或基准价格过期，重新获取
        if (basePrices.isEmpty() || basePriceTime == null || 
            ChronoUnit.HOURS.between(basePriceTime, LocalDateTime.now()) > 24) {
            refreshBasePrices();
        }

        // 计算加权平均涨跌幅
        double totalWeightedChange = 0;
        double totalVolume = 0;
        int validCount = 0;

        for (TickerData ticker : tickers) {
            Double basePrice = basePrices.get(ticker.getSymbol());
            if (basePrice == null || basePrice <= 0) {
                continue;
            }

            double changePercent = (ticker.getLastPrice() - basePrice) / basePrice * 100;
            double volume = ticker.getQuoteVolume();

            // 过滤异常值
            if (Math.abs(changePercent) > 200 || volume <= 0) {
                continue;
            }

            totalWeightedChange += changePercent * volume;
            totalVolume += volume;
            validCount++;
        }

        if (totalVolume <= 0 || validCount == 0) {
            log.warn("无有效数据计算指数");
            return null;
        }

        double indexValue = totalWeightedChange / totalVolume;

        // 时间对齐到5分钟
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime alignedTime = alignToFiveMinutes(now);

        // 检查是否已存在该时间点的数据
        if (marketIndexRepository.existsByTimestamp(alignedTime)) {
            log.debug("时间点 {} 已存在数据，跳过", alignedTime);
            return null;
        }

        MarketIndex index = new MarketIndex(alignedTime, indexValue, totalVolume, validCount);
        marketIndexRepository.save(index);

        log.info("保存指数: 时间={}, 值={:.4f}%, 币种数={}", alignedTime, indexValue, validCount);
        return index;
    }

    /**
     * 刷新基准价格（获取每个币种3天前的价格）
     */
    public void refreshBasePrices() {
        log.info("刷新基准价格...");
        basePrices.clear();

        List<String> symbols = binanceApiService.getAllUsdtSymbols();
        long now = System.currentTimeMillis();
        long threeDaysAgo = now - 3L * 24 * 60 * 60 * 1000;

        int count = 0;
        for (String symbol : symbols) {
            try {
                // 只获取3天前那个时间点附近的1根K线
                List<KlineData> klines = binanceApiService.getKlines(
                        symbol, "5m", threeDaysAgo, threeDaysAgo + 300000, 1);
                
                if (!klines.isEmpty()) {
                    basePrices.put(symbol, klines.get(0).getClosePrice());
                    count++;
                }

                // 请求间隔
                Thread.sleep(binanceApiService.getRequestIntervalMs());
            } catch (Exception e) {
                log.error("获取基准价格失败 {}: {}", symbol, e.getMessage());
            }
        }

        basePriceTime = LocalDateTime.now();
        log.info("基准价格刷新完成，共 {} 个币种", count);
    }

    /**
     * 回补历史数据
     * @param days 回补天数
     */
    public void backfillHistoricalData(int days) {
        log.info("开始回补 {} 天历史数据...", days);

        List<String> symbols = binanceApiService.getAllUsdtSymbols();
        long now = System.currentTimeMillis();
        long startTime = now - (long) days * 24 * 60 * 60 * 1000;

        // 存储每个时间点的所有币种数据: timestamp -> (symbol -> KlineData)
        Map<Long, Map<String, KlineData>> timeSeriesData = new TreeMap<>();

        int processedCount = 0;
        for (String symbol : symbols) {
            try {
                List<KlineData> klines = binanceApiService.getKlinesWithPagination(
                        symbol, "5m", startTime, now, 500);

                for (KlineData kline : klines) {
                    long timestamp = kline.getTimestamp()
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();

                    timeSeriesData.computeIfAbsent(timestamp, k -> new HashMap<>())
                            .put(symbol, kline);
                }

                processedCount++;
                if (processedCount % 50 == 0) {
                    log.info("已处理 {}/{} 个币种", processedCount, symbols.size());
                }

            } catch (Exception e) {
                log.error("获取K线失败 {}: {}", symbol, e.getMessage());
            }
        }

        log.info("K线数据获取完成，共 {} 个时间点，开始计算指数...", timeSeriesData.size());

        // 计算每个时间点的指数
        // 需要先确定基准价格（最早时间点的价格）
        Map<String, Double> historicalBasePrices = new HashMap<>();
        Long firstTimestamp = timeSeriesData.keySet().stream().findFirst().orElse(null);
        
        if (firstTimestamp != null) {
            Map<String, KlineData> firstData = timeSeriesData.get(firstTimestamp);
            for (Map.Entry<String, KlineData> entry : firstData.entrySet()) {
                historicalBasePrices.put(entry.getKey(), entry.getValue().getOpenPrice());
            }
            
            // 更新全局基准价格
            basePrices = new HashMap<>(historicalBasePrices);
            basePriceTime = LocalDateTime.now();
        }

        // 计算每个时间点的指数
        List<MarketIndex> indexList = new ArrayList<>();
        
        for (Map.Entry<Long, Map<String, KlineData>> entry : timeSeriesData.entrySet()) {
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(entry.getKey()),
                    ZoneId.systemDefault());

            // 跳过已存在的数据
            if (marketIndexRepository.existsByTimestamp(timestamp)) {
                continue;
            }

            Map<String, KlineData> symbolData = entry.getValue();
            
            double totalWeightedChange = 0;
            double totalVolume = 0;
            int validCount = 0;

            for (Map.Entry<String, KlineData> klineEntry : symbolData.entrySet()) {
                String symbol = klineEntry.getKey();
                KlineData kline = klineEntry.getValue();
                Double basePrice = historicalBasePrices.get(symbol);

                if (basePrice == null || basePrice <= 0) {
                    continue;
                }

                double changePercent = (kline.getClosePrice() - basePrice) / basePrice * 100;
                double volume = kline.getVolume();

                // 过滤异常值
                if (Math.abs(changePercent) > 200 || volume <= 0) {
                    continue;
                }

                totalWeightedChange += changePercent * volume;
                totalVolume += volume;
                validCount++;
            }

            if (totalVolume > 0 && validCount > 0) {
                double indexValue = totalWeightedChange / totalVolume;
                indexList.add(new MarketIndex(timestamp, indexValue, totalVolume, validCount));
            }
        }

        // 批量保存
        if (!indexList.isEmpty()) {
            marketIndexRepository.saveAll(indexList);
            log.info("历史数据回补完成，共保存 {} 条记录", indexList.size());
        } else {
            log.info("无新数据需要保存");
        }
    }

    /**
     * 获取历史指数数据
     */
    public List<MarketIndex> getHistoryData(int hours) {
        LocalDateTime start = LocalDateTime.now().minusHours(hours);
        return marketIndexRepository.findByTimestampAfterOrderByTimestampAsc(start);
    }

    /**
     * 获取最新指数
     */
    public MarketIndex getLatestIndex() {
        return marketIndexRepository.findTopByOrderByTimestampDesc().orElse(null);
    }

    /**
     * 时间对齐到5分钟
     */
    private LocalDateTime alignToFiveMinutes(LocalDateTime time) {
        int minute = time.getMinute();
        int alignedMinute = (minute / 5) * 5;
        return time.withMinute(alignedMinute).withSecond(0).withNano(0);
    }
}
