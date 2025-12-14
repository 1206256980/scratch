package com.binance.index.service;

import com.binance.index.dto.KlineData;
import com.binance.index.entity.MarketIndex;
import com.binance.index.repository.MarketIndexRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Service
public class IndexCalculatorService {

    private static final Logger log = LoggerFactory.getLogger(IndexCalculatorService.class);

    private final BinanceApiService binanceApiService;
    private final MarketIndexRepository marketIndexRepository;
    private final ExecutorService executorService;

    // 缓存各币种的基准价格（回补起始时间的价格）
    private Map<String, Double> basePrices = new HashMap<>();
    private LocalDateTime basePriceTime;

    public IndexCalculatorService(BinanceApiService binanceApiService,
            MarketIndexRepository marketIndexRepository,
            ExecutorService klineExecutorService) {
        this.binanceApiService = binanceApiService;
        this.marketIndexRepository = marketIndexRepository;
        this.executorService = klineExecutorService;
    }

    /**
     * 计算并保存当前时刻的市场指数（实时采集用）
     * 使用并发获取K线数据，获取准确的5分钟成交额
     */
    public MarketIndex calculateAndSaveCurrentIndex() {
        // 如果没有基准价格，需要先刷新
        if (basePrices.isEmpty()) {
            log.warn("基准价格为空，等待回补完成或手动刷新");
            return null;
        }

        // 时间对齐到5分钟（使用UTC时间）
        LocalDateTime now = LocalDateTime.now(java.time.ZoneOffset.UTC);
        LocalDateTime alignedTime = alignToFiveMinutes(now);

        // 检查是否已存在该时间点的数据（防止与回补数据重叠）
        if (marketIndexRepository.existsByTimestamp(alignedTime)) {
            log.debug("时间点 {} 已存在数据，跳过", alignedTime);
            return null;
        }

        // 获取所有需要处理的币种
        List<String> symbols = binanceApiService.getAllUsdtSymbols();
        if (symbols.isEmpty()) {
            log.warn("无法获取交易对列表");
            return null;
        }

        log.info("开始并发获取 {} 个币种的K线数据...", symbols.size());
        long startTime = System.currentTimeMillis();

        // 并发获取所有币种的最新K线
        List<CompletableFuture<KlineData>> futures = symbols.stream()
                .map(symbol -> CompletableFuture.supplyAsync(
                        () -> binanceApiService.getLatestKline(symbol),
                        executorService))
                .collect(Collectors.toList());

        // 等待所有请求完成
        List<KlineData> allKlines = futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("K线数据获取完成，成功 {} 个，耗时 {}ms", allKlines.size(), elapsed);

        // 计算指数和成交额
        double totalChange = 0;
        double totalVolume = 0;
        int validCount = 0;
        int upCount = 0; // 上涨币种数
        int downCount = 0; // 下跌币种数

        for (KlineData kline : allKlines) {
            String symbol = kline.getSymbol();
            Double basePrice = basePrices.get(symbol);

            // 新币处理：如果没有基准价格，使用当前价格作为基准
            if (basePrice == null || basePrice <= 0) {
                if (kline.getClosePrice() > 0) {
                    basePrices.put(symbol, kline.getClosePrice());
                    log.info("新币种 {} 设置基准价格: {}", symbol, kline.getClosePrice());
                }
                continue; // 第一次采集时跳过计算，下次开始参与
            }

            // 计算相对于基准时间的涨跌幅
            double changePercent = (kline.getClosePrice() - basePrice) / basePrice * 100;
            double volume = kline.getVolume(); // 5分钟成交额

            // 过滤异常值
            if (Math.abs(changePercent) > 200 || volume <= 0) {
                continue;
            }

            // 统计涨跌数量
            if (changePercent > 0) {
                upCount++;
            } else if (changePercent < 0) {
                downCount++;
            }

            totalChange += changePercent;
            totalVolume += volume;
            validCount++;
        }

        if (validCount == 0) {
            log.warn("无有效数据计算指数");
            return null;
        }

        // 简单平均
        double indexValue = totalChange / validCount;

        // 计算涨跌比率
        double adr = downCount > 0 ? (double) upCount / downCount : upCount;

        // 再次检查是否已存在该时间点的数据（双重检查）
        if (marketIndexRepository.existsByTimestamp(alignedTime)) {
            log.debug("时间点 {} 已存在数据（并发写入），跳过", alignedTime);
            return null;
        }

        MarketIndex index = new MarketIndex(alignedTime, indexValue, totalVolume, validCount, upCount, downCount, adr);
        marketIndexRepository.save(index);

        log.info("保存指数: 时间={}, 值={}%, 涨/跌={}/{}, ADR={}, 币种数={}",
                alignedTime, String.format("%.4f", indexValue),
                upCount, downCount, String.format("%.2f", adr), validCount);
        return index;
    }

    /**
     * 刷新基准价格（获取每个币种回补起始时间的价格）
     */
    public void refreshBasePrices() {
        log.info("刷新基准价格...");
        basePrices.clear();

        List<String> symbols = binanceApiService.getAllUsdtSymbols();
        long now = System.currentTimeMillis();
        long backfillStartTime = now - 7L * 24 * 60 * 60 * 1000; // 7天前

        int count = 0;
        for (String symbol : symbols) {
            try {
                // 只获取回补起始时间点附近的1根K线
                List<KlineData> klines = binanceApiService.getKlines(
                        symbol, "5m", backfillStartTime, backfillStartTime + 300000, 1);

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
     * 
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
        int failedCount = 0;
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
                    log.info("已处理 {}/{} 个币种（失败：{}）", processedCount, symbols.size(), failedCount);
                }

            } catch (Exception e) {
                failedCount++;
                log.error("获取K线失败 {}: {} - {}", symbol, e.getClass().getSimpleName(), e.getMessage());
                log.debug("详细错误堆栈：", e);

                // 如果连续失败多次，可能是被限流，等待更长时间
                if (failedCount % 10 == 0) {
                    log.warn("连续失败 {} 次，等待5秒后继续...", failedCount);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        log.info("K线数据获取完成，成功：{}，失败：{}，共 {} 个时间点",
                processedCount, failedCount, timeSeriesData.size());

        // 计算每个时间点的指数
        // 需要先确定基准价格：为每个币种找最早出现的价格（处理新币种）
        Map<String, Double> historicalBasePrices = new HashMap<>();

        // 遍历所有时间点（按时间顺序），为每个币种找最早出现的价格
        for (Map<String, KlineData> symbolData : timeSeriesData.values()) {
            for (Map.Entry<String, KlineData> entry : symbolData.entrySet()) {
                String symbol = entry.getKey();
                // 如果还没有这个币种的基准价格，就用这个（它是最早的）
                if (!historicalBasePrices.containsKey(symbol)) {
                    historicalBasePrices.put(symbol, entry.getValue().getOpenPrice());
                }
            }
        }

        // 更新全局基准价格
        if (!historicalBasePrices.isEmpty()) {
            basePrices = new HashMap<>(historicalBasePrices);
            basePriceTime = LocalDateTime.now();
            log.info("基准价格设置完成，共 {} 个币种", basePrices.size());
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

            double totalChange = 0;
            double totalVolume = 0;
            int validCount = 0;
            int upCount = 0;
            int downCount = 0;

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

                // 统计涨跌数量
                if (changePercent > 0) {
                    upCount++;
                } else if (changePercent < 0) {
                    downCount++;
                }

                totalChange += changePercent;
                totalVolume += volume;
                validCount++;
            }

            if (validCount > 0) {
                // 简单平均
                double indexValue = totalChange / validCount;
                double adr = downCount > 0 ? (double) upCount / downCount : upCount;
                indexList.add(new MarketIndex(timestamp, indexValue, totalVolume, validCount, upCount, downCount, adr));
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
