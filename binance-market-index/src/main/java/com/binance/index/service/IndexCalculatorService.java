package com.binance.index.service;

import com.binance.index.dto.DistributionBucket;
import com.binance.index.dto.DistributionData;
import com.binance.index.dto.KlineData;
import com.binance.index.entity.BasePrice;
import com.binance.index.entity.CoinPrice;
import com.binance.index.entity.MarketIndex;
import com.binance.index.repository.CoinPriceRepository;
import com.binance.index.repository.JdbcCoinPriceRepository;
import com.binance.index.repository.BasePriceRepository;
import com.binance.index.repository.MarketIndexRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Service
public class IndexCalculatorService {

    private static final Logger log = LoggerFactory.getLogger(IndexCalculatorService.class);

    private final BinanceApiService binanceApiService;
    private final MarketIndexRepository marketIndexRepository;
    private final CoinPriceRepository coinPriceRepository;
    private final JdbcCoinPriceRepository jdbcCoinPriceRepository;
    private final BasePriceRepository basePriceRepository;
    private final ExecutorService executorService;

    // 缓存各币种的基准价格（回补起始时间的价格）
    private Map<String, Double> basePrices = new HashMap<>();
    private LocalDateTime basePriceTime;

    // 回补状态标志
    private volatile boolean backfillInProgress = false;

    // 回补期间的缓冲队列：暂存实时采集的K线原始数据，回补完成后再计算并保存
    private final ConcurrentLinkedQueue<BufferedKlineData> pendingKlineQueue = new ConcurrentLinkedQueue<>();

    // 内部类：用于暂存原始K线数据（不计算指数，等回补完成后再计算）
    private static class BufferedKlineData {
        final LocalDateTime timestamp;
        final List<KlineData> klines;

        BufferedKlineData(LocalDateTime timestamp, List<KlineData> klines) {
            this.timestamp = timestamp;
            this.klines = klines;
        }
    }

    public IndexCalculatorService(BinanceApiService binanceApiService,
            MarketIndexRepository marketIndexRepository,
            CoinPriceRepository coinPriceRepository,
            JdbcCoinPriceRepository jdbcCoinPriceRepository,
            BasePriceRepository basePriceRepository,
            ExecutorService klineExecutorService) {
        this.binanceApiService = binanceApiService;
        this.marketIndexRepository = marketIndexRepository;
        this.coinPriceRepository = coinPriceRepository;
        this.jdbcCoinPriceRepository = jdbcCoinPriceRepository;
        this.basePriceRepository = basePriceRepository;
        this.executorService = klineExecutorService;
    }

    /**
     * 设置回补状态
     */
    public void setBackfillInProgress(boolean inProgress) {
        this.backfillInProgress = inProgress;
    }

    /**
     * 检查回补是否正在进行
     */
    public boolean isBackfillInProgress() {
        return backfillInProgress;
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

        // 预测最新闭合K线的时间：当前时间对齐到5分钟后减5分钟
        // 例如：09:07 → 对齐到09:05 → 减5分钟 → 09:00（这是最新闭合K线的openTime）
        LocalDateTime now = LocalDateTime.now(java.time.ZoneOffset.UTC);
        LocalDateTime expectedKlineTime = alignToFiveMinutes(now).minusMinutes(5);

        // 检查是否已存在该时间点的数据（在调用API之前先检查，避免浪费资源）
        if (marketIndexRepository.existsByTimestamp(expectedKlineTime)) {
            log.debug("时间点 {} 已存在数据，跳过采集", expectedKlineTime);
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

        if (allKlines.isEmpty()) {
            log.warn("无有效K线数据");
            return null;
        }

        // 使用K线本身的timestamp（所有K线应该是同一时间点的）
        LocalDateTime klineTime = allKlines.get(0).getTimestamp();

        // 再次检查是否已存在该时间点的数据（双重检查，防止并发）
        if (marketIndexRepository.existsByTimestamp(klineTime)) {
            log.debug("时间点 {} 已存在数据，跳过", klineTime);
            return null;
        }

        // 计算指数和成交额
        double totalChange = 0;
        double totalVolume = 0;
        int validCount = 0;
        int upCount = 0; // 上涨币种数
        int downCount = 0; // 下跌币种数

        for (KlineData kline : allKlines) {
            String symbol = kline.getSymbol();
            Double basePrice = basePrices.get(symbol);

            // 新币处理：如果没有基准价格，使用当前价格作为基准并保存到数据库
            if (basePrice == null || basePrice <= 0) {
                if (kline.getClosePrice() > 0) {
                    basePrices.put(symbol, kline.getClosePrice());
                    // 同时保存到数据库
                    basePriceRepository.save(new BasePrice(symbol, kline.getClosePrice()));
                    log.info("新币种 {} 设置基准价格: {} (已保存到数据库)", symbol, kline.getClosePrice());
                }
                continue; // 第一次采集时跳过计算，下次开始参与
            }

            // 计算相对于基准时间的涨跌幅
            double changePercent = (kline.getClosePrice() - basePrice) / basePrice * 100;
            double volume = kline.getVolume(); // 5分钟成交额

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
        if (marketIndexRepository.existsByTimestamp(klineTime)) {
            log.debug("时间点 {} 已存在数据（并发写入），跳过", klineTime);
            return null;
        }

        MarketIndex index = new MarketIndex(klineTime, indexValue, totalVolume, validCount, upCount, downCount, adr);
        marketIndexRepository.save(index);

        // 保存每个币种的OHLC价格（使用K线本身的timestamp）
        List<CoinPrice> coinPrices = allKlines.stream()
                .filter(k -> k.getClosePrice() > 0)
                .map(k -> new CoinPrice(k.getSymbol(), k.getTimestamp(),
                        k.getOpenPrice(), k.getHighPrice(), k.getLowPrice(), k.getClosePrice()))
                .collect(Collectors.toList());
        jdbcCoinPriceRepository.batchInsert(coinPrices);
        log.debug("保存 {} 个币种价格", coinPrices.size());

        log.info("保存指数: 时间={}, 值={}%, 涨/跌={}/{}, ADR={}, 币种数={}",
                klineTime, String.format("%.4f", indexValue),
                upCount, downCount, String.format("%.2f", adr), validCount);
        return index;
    }

    /**
     * 回补期间采集数据并暂存到内存队列（只保存原始K线，不计算指数）
     * 等回补完成后，基准价格设置好了再计算指数并保存
     */
    public void collectAndBuffer() {
        // 预测最新闭合K线的时间
        LocalDateTime now = LocalDateTime.now(java.time.ZoneOffset.UTC);
        LocalDateTime expectedKlineTime = alignToFiveMinutes(now).minusMinutes(5);

        // 检查是否已存在该时间点的数据（避免重复采集）
        if (marketIndexRepository.existsByTimestamp(expectedKlineTime)) {
            log.debug("时间点 {} 已存在数据，跳过暂存采集", expectedKlineTime);
            return;
        }

        // 检查队列中是否已有该时间点的数据（避免重复暂存）
        for (BufferedKlineData buffered : pendingKlineQueue) {
            if (buffered.timestamp.equals(expectedKlineTime)) {
                log.debug("时间点 {} 已在暂存队列中，跳过", expectedKlineTime);
                return;
            }
        }

        // 获取所有需要处理的币种
        List<String> symbols = binanceApiService.getAllUsdtSymbols();
        if (symbols.isEmpty()) {
            log.warn("无法获取交易对列表");
            return;
        }

        log.info("回补期间暂存采集：开始获取 {} 个币种的K线数据...", symbols.size());
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
        log.info("暂存采集K线数据完成，成功 {} 个，耗时 {}ms", allKlines.size(), elapsed);

        if (allKlines.isEmpty()) {
            log.warn("无有效K线数据");
            return;
        }

        // 使用K线本身的timestamp
        LocalDateTime klineTime = allKlines.get(0).getTimestamp();

        // 再次检查（双重检查，防止并发）
        if (marketIndexRepository.existsByTimestamp(klineTime)) {
            log.debug("时间点 {} 已存在数据，跳过暂存", klineTime);
            return;
        }

        // 检查队列中是否已有该时间点
        for (BufferedKlineData buffered : pendingKlineQueue) {
            if (buffered.timestamp.equals(klineTime)) {
                log.debug("时间点 {} 已在暂存队列中，跳过", klineTime);
                return;
            }
        }

        // 只暂存原始K线数据，不计算指数（因为基准价格可能还没设置好）
        pendingKlineQueue.offer(new BufferedKlineData(klineTime, allKlines));
        log.info("K线数据已暂存: 时间={}, 队列大小={}", klineTime, pendingKlineQueue.size());
    }

    /**
     * 将暂存队列中的K线数据计算指数后保存到数据库（回补完成后调用）
     * 此时基准价格已经设置好，可以正确计算指数
     */
    public void flushPendingData() {
        if (pendingKlineQueue.isEmpty()) {
            log.info("暂存队列为空，无需刷新");
            return;
        }

        log.info("开始处理 {} 条暂存K线数据...", pendingKlineQueue.size());
        int savedIndexCount = 0;
        int savedPriceCount = 0;
        int skippedCount = 0;

        BufferedKlineData bufferedData;
        while ((bufferedData = pendingKlineQueue.poll()) != null) {
            LocalDateTime timestamp = bufferedData.timestamp;

            // 检查是否已存在（避免与回补数据重复）
            if (marketIndexRepository.existsByTimestamp(timestamp)) {
                log.debug("时间点 {} 已存在（回补数据），跳过暂存数据", timestamp);
                skippedCount++;
                continue;
            }

            // 使用当前的基准价格计算指数
            List<KlineData> allKlines = bufferedData.klines;

            double totalChange = 0;
            double totalVolume = 0;
            int validCount = 0;
            int upCount = 0;
            int downCount = 0;
            List<CoinPrice> coinPrices = new ArrayList<>();

            for (KlineData kline : allKlines) {
                String symbol = kline.getSymbol();
                Double basePrice = basePrices.get(symbol);

                if (basePrice == null || basePrice <= 0) {
                    continue;
                }

                double changePercent = (kline.getClosePrice() - basePrice) / basePrice * 100;
                double volume = kline.getVolume();

                if (changePercent > 0) {
                    upCount++;
                } else if (changePercent < 0) {
                    downCount++;
                }

                totalChange += changePercent;
                totalVolume += volume;
                validCount++;

                if (kline.getClosePrice() > 0) {
                    coinPrices.add(new CoinPrice(kline.getSymbol(), kline.getTimestamp(),
                            kline.getOpenPrice(), kline.getHighPrice(), kline.getLowPrice(), kline.getClosePrice()));
                }
            }

            if (validCount == 0) {
                log.warn("暂存数据 {} 无有效币种计算指数，跳过", timestamp);
                skippedCount++;
                continue;
            }

            double indexValue = totalChange / validCount;
            double adr = downCount > 0 ? (double) upCount / downCount : upCount;
            MarketIndex index = new MarketIndex(timestamp, indexValue, totalVolume, validCount, upCount, downCount,
                    adr);

            // 保存指数
            marketIndexRepository.save(index);
            savedIndexCount++;

            // 保存币种价格
            if (!coinPrices.isEmpty()) {
                jdbcCoinPriceRepository.batchInsert(coinPrices);
                savedPriceCount += coinPrices.size();
            }

            log.debug("暂存数据已保存: 时间={}", timestamp);
        }

        log.info("暂存数据刷新完成: 保存指数 {} 条, 价格 {} 条, 跳过 {} 条（已存在或无效）",
                savedIndexCount, savedPriceCount, skippedCount);
    }

    /**
     * 回补历史数据
     *
     * @param days 回补天数
     */
    public void backfillHistoricalData(int days) {
        log.info("开始回补 {} 天历史数据...", days);

        // 首先尝试从数据库加载基准价格
        List<BasePrice> existingBasePrices = basePriceRepository.findAll();
        if (!existingBasePrices.isEmpty()) {
            basePrices = existingBasePrices.stream()
                    .collect(Collectors.toMap(BasePrice::getSymbol, BasePrice::getPrice, (a, b) -> a));
            basePriceTime = existingBasePrices.get(0).getCreatedAt();
            log.info("从数据库加载基准价格成功，共 {} 个币种，创建时间: {}", basePrices.size(), basePriceTime);
        } else {
            log.info("数据库中没有基准价格，将从历史数据计算");
        }

        List<String> symbols = binanceApiService.getAllUsdtSymbols();
        long now = System.currentTimeMillis();

        // 对齐到上一个5分钟边界（确保只获取已闭合的K线）
        // 例如：当前 09:02，对齐到 09:00，这样只会拿到 <=08:55 的已闭合K线
        long fiveMinutesMs = 5 * 60 * 1000;
        long alignedEndTime = (now / fiveMinutesMs) * fiveMinutesMs;
        long startTime = alignedEndTime - (long) days * 24 * 60 * 60 * 1000;

        log.info("回补时间范围: {} -> {} (对齐到5分钟边界)",
                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(startTime), ZoneId.systemDefault()),
                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(alignedEndTime), ZoneId.systemDefault()));

        // 存储每个时间点的所有币种数据: timestamp -> (symbol -> KlineData)
        Map<Long, Map<String, KlineData>> timeSeriesData = new TreeMap<>();

        int processedCount = 0;
        int failedCount = 0;
        for (String symbol : symbols) {
            try {
                List<KlineData> klines = binanceApiService.getKlinesWithPagination(
                        symbol, "5m", startTime, alignedEndTime, 500);

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

        // 更新全局基准价格并保存到数据库
        if (!historicalBasePrices.isEmpty()) {
            basePrices = new HashMap<>(historicalBasePrices);
            basePriceTime = LocalDateTime.now();

            // 保存到数据库（仅当数据库中没有基准价格时）
            if (basePriceRepository.count() == 0) {
                List<BasePrice> basePriceList = historicalBasePrices.entrySet().stream()
                        .map(e -> new BasePrice(e.getKey(), e.getValue()))
                        .collect(Collectors.toList());
                basePriceRepository.saveAll(basePriceList);
                log.info("基准价格已保存到数据库，共 {} 个币种", basePriceList.size());
            }
            log.info("基准价格设置完成，共 {} 个币种", basePrices.size());
        }

        // 计算每个时间点的指数
        List<MarketIndex> indexList = new ArrayList<>();

        // 批量查询已存在的时间戳（优化：1次查询替代2000+次查询）
        LocalDateTime backfillStart = LocalDateTime.now().minusDays(days);
        LocalDateTime backfillEnd = LocalDateTime.now();
        Set<LocalDateTime> existingIndexTimestamps = new HashSet<>(
                marketIndexRepository.findAllTimestampsBetween(backfillStart, backfillEnd));
        Set<LocalDateTime> existingPriceTimestamps = new HashSet<>(
                coinPriceRepository.findAllDistinctTimestampsBetween(backfillStart, backfillEnd));
        log.info("已存在指数时间点: {} 个, 价格时间点: {} 个",
                existingIndexTimestamps.size(), existingPriceTimestamps.size());

        for (Map.Entry<Long, Map<String, KlineData>> entry : timeSeriesData.entrySet()) {
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(entry.getKey()),
                    ZoneId.systemDefault());

            // 跳过已存在的数据（使用内存Set快速判断）
            if (existingIndexTimestamps.contains(timestamp)) {
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
            log.info("历史指数数据回补完成，共保存 {} 条记录", indexList.size());
        } else {
            log.info("无新指数数据需要保存");
        }

        // 保存每个时间点的币种价格
        if (!existingPriceTimestamps.isEmpty()) {
            LocalDateTime earliest = existingPriceTimestamps.stream().min(LocalDateTime::compareTo).orElse(null);
            LocalDateTime latest = existingPriceTimestamps.stream().max(LocalDateTime::compareTo).orElse(null);
            log.info("历史已有 {} 个时间点的价格数据，最早: {}，最晚: {}，将跳过这些时间点",
                    existingPriceTimestamps.size(), earliest, latest);
        }
        log.info("开始保存币种价格历史...");
        List<CoinPrice> allCoinPrices = new ArrayList<>();
        for (Map.Entry<Long, Map<String, KlineData>> entry : timeSeriesData.entrySet()) {
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(entry.getKey()),
                    ZoneId.systemDefault());

            // 跳过已有价格数据的时间点（使用内存Set快速判断）
            if (existingPriceTimestamps.contains(timestamp)) {
                continue;
            }

            for (Map.Entry<String, KlineData> klineEntry : entry.getValue().entrySet()) {
                KlineData kline = klineEntry.getValue();
                if (kline.getClosePrice() > 0) {
                    allCoinPrices.add(new CoinPrice(kline.getSymbol(), timestamp,
                            kline.getOpenPrice(), kline.getHighPrice(), kline.getLowPrice(), kline.getClosePrice()));
                }
            }

            // 每1万条批量保存一次（JDBC批量插入更高效）
            if (allCoinPrices.size() >= 10000) {
                jdbcCoinPriceRepository.batchInsert(allCoinPrices);
                allCoinPrices.clear();
            }
        }
        // 保存剩余的
        if (!allCoinPrices.isEmpty()) {
            jdbcCoinPriceRepository.batchInsert(allCoinPrices);
            log.info("币种价格保存完成");
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
     * 查询指定币种的历史价格数据（调试用）
     */
    public List<CoinPrice> getCoinPriceHistory(String symbol, LocalDateTime startTime) {
        return coinPriceRepository.findBySymbolAndTimeRange(symbol, startTime);
    }

    /**
     * 获取所有基准价格（调试用）
     */
    public List<BasePrice> getAllBasePrices() {
        return basePriceRepository.findAll();
    }

    /**
     * 时间对齐到5分钟
     */
    private LocalDateTime alignToFiveMinutes(LocalDateTime time) {
        int minute = time.getMinute();
        int alignedMinute = (minute / 5) * 5;
        return time.withMinute(alignedMinute).withSecond(0).withNano(0);
    }

    /**
     * 获取涨幅分布数据（从数据库读取，速度快）
     *
     * @param hours 基准时间（多少小时前），支持小数
     * @return 涨幅分布数据
     */
    public DistributionData getDistribution(double hours) {
        log.info("计算涨幅分布，基准时间: {}小时前", hours);

        long now = System.currentTimeMillis();
        // 转换为分钟以支持小数小时
        long minutes = (long) (hours * 60);

        // 从数据库获取最新价格
        List<CoinPrice> latestPrices = coinPriceRepository.findLatestPrices();
        if (latestPrices.isEmpty()) {
            log.warn("数据库中没有价格数据，可能需要等待回补完成");
            return null;
        }

        // 获取最新数据时间（用于获取当前价格和计算最高/最低价区间）
        LocalDateTime latestTime = latestPrices.get(0).getTimestamp();

        // 基于当前系统时间（对齐到5分钟边界）计算基准时间
        // 例如：系统时间11:42 → 对齐到11:40 → 减15分钟 → 基准时间11:25
        LocalDateTime alignedNow = alignToFiveMinutes(LocalDateTime.now());
        LocalDateTime baseTime = alignedNow.minusMinutes(minutes);

        // 从数据库获取基准时间的价格
        List<CoinPrice> basePriceList = coinPriceRepository.findEarliestPricesAfter(baseTime);
        if (basePriceList.isEmpty()) {
            log.warn("找不到基准时间 {} 的价格数据", baseTime);
            return null;
        }

        // 调试：打印关键时间点
        LocalDateTime actualBaseTime = basePriceList.get(0).getTimestamp();
        log.info("[时间调试] 系统时间={}, 对齐时间={}, 最新数据时间={}, 期望基准时间={}, 实际基准时间={}",
                LocalDateTime.now(), alignedNow, latestTime, baseTime, actualBaseTime);

        // 转换为Map便于查找
        // 当前价格使用收盘价
        Map<String, Double> currentPriceMap = latestPrices.stream()
                .collect(Collectors.toMap(CoinPrice::getSymbol, CoinPrice::getPrice, (a, b) -> a));
        // 基准价格使用开盘价（更准确地反映起点）
        Map<String, Double> basePriceMap = basePriceList.stream()
                .filter(cp -> cp.getOpenPrice() != null)
                .collect(Collectors.toMap(CoinPrice::getSymbol, CoinPrice::getOpenPrice, (a, b) -> a));

        // 获取时间区间内的最高/最低价格
        List<Object[]> maxPricesResult = coinPriceRepository.findMaxPricesBySymbolInRange(baseTime, latestTime);
        List<Object[]> minPricesResult = coinPriceRepository.findMinPricesBySymbolInRange(baseTime, latestTime);

        Map<String, Double> maxPriceMap = maxPricesResult.stream()
                .collect(Collectors.toMap(
                        row -> (String) row[0],
                        row -> (Double) row[1],
                        (a, b) -> a));
        Map<String, Double> minPriceMap = minPricesResult.stream()
                .collect(Collectors.toMap(
                        row -> (String) row[0],
                        row -> (Double) row[1],
                        (a, b) -> a));

        log.info("从数据库获取价格: 当前={} 个, 基准={} 个, 最高={} 个, 最低={} 个",
                currentPriceMap.size(), basePriceMap.size(), maxPriceMap.size(), minPriceMap.size());

        // 计算涨跌幅（当前、最高、最低）
        Map<String, Double> changeMap = new HashMap<>();
        Map<String, Double> maxChangeMap = new HashMap<>();
        Map<String, Double> minChangeMap = new HashMap<>();

        for (Map.Entry<String, Double> entry : currentPriceMap.entrySet()) {
            String symbol = entry.getKey();
            Double currentPrice = entry.getValue();
            Double basePrice = basePriceMap.get(symbol);
            Double maxPrice = maxPriceMap.get(symbol);
            Double minPrice = minPriceMap.get(symbol);

            if (basePrice != null && basePrice > 0 && currentPrice != null && currentPrice > 0) {
                double changePercent = (currentPrice - basePrice) / basePrice * 100;
                changeMap.put(symbol, changePercent);

                // 计算最高涨跌幅
                if (maxPrice != null && maxPrice > 0) {
                    double maxChangePercent = (maxPrice - basePrice) / basePrice * 100;
                    maxChangeMap.put(symbol, maxChangePercent);
                }

                // 计算最低涨跌幅
                if (minPrice != null && minPrice > 0) {
                    double minChangePercent = (minPrice - basePrice) / basePrice * 100;
                    minChangeMap.put(symbol, minChangePercent);
                }

            }
        }

        log.info("涨跌幅计算完成: {} 个币种", changeMap.size());

        if (changeMap.isEmpty()) {
            log.warn("没有有效的涨跌幅数据");
            return null;
        }

        // 计算涨跌幅范围
        double minChange = changeMap.values().stream().mapToDouble(Double::doubleValue).min().orElse(0);
        double maxChange = changeMap.values().stream().mapToDouble(Double::doubleValue).max().orElse(0);

        // 根据数据范围动态确定区间大小
        double range = maxChange - minChange;
        double bucketSize;
        if (range <= 2) {
            bucketSize = 0.2; // 0.2%区间，适合分钟级
        } else if (range <= 5) {
            bucketSize = 0.5; // 0.5%区间
        } else if (range <= 20) {
            bucketSize = 1; // 1%区间
        } else if (range <= 50) {
            bucketSize = 2; // 2%区间
        } else {
            bucketSize = 5; // 5%区间，适合长期
        }

        // 计算区间边界
        double bucketMin = Math.floor(minChange / bucketSize) * bucketSize;
        double bucketMax = Math.ceil(maxChange / bucketSize) * bucketSize;

        // 创建区间
        Map<String, List<String>> buckets = new LinkedHashMap<>();
        for (double start = bucketMin; start < bucketMax; start += bucketSize) {
            String rangeKey;
            if (bucketSize < 1) {
                rangeKey = String.format("%.1f%%~%.1f%%", start, start + bucketSize);
            } else {
                rangeKey = String.format("%.0f%%~%.0f%%", start, start + bucketSize);
            }
            buckets.put(rangeKey, new ArrayList<>());
        }

        // 分配币种到区间
        int upCount = 0;
        int downCount = 0;

        for (Map.Entry<String, Double> entry : changeMap.entrySet()) {
            String symbol = entry.getKey();
            double change = entry.getValue();

            if (change > 0)
                upCount++;
            else if (change < 0)
                downCount++;

            // 计算所属区间
            double bucketStart = Math.floor(change / bucketSize) * bucketSize;
            String rangeKey;
            if (bucketSize < 1) {
                rangeKey = String.format("%.1f%%~%.1f%%", bucketStart, bucketStart + bucketSize);
            } else {
                rangeKey = String.format("%.0f%%~%.0f%%", bucketStart, bucketStart + bucketSize);
            }

            if (buckets.containsKey(rangeKey)) {
                buckets.get(rangeKey).add(symbol);
            }
        }

        // 构建响应
        DistributionData data = new DistributionData();
        data.setTimestamp(now);
        data.setTotalCoins(changeMap.size());
        data.setUpCount(upCount);
        data.setDownCount(downCount);

        List<DistributionBucket> distribution = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : buckets.entrySet()) {
            List<String> coins = entry.getValue();

            // 构建带涨跌幅的详情列表，并按涨跌幅排序
            List<DistributionBucket.CoinDetail> coinDetails = coins.stream()
                    .map(symbol -> new DistributionBucket.CoinDetail(
                            symbol,
                            changeMap.getOrDefault(symbol, 0.0),
                            maxChangeMap.getOrDefault(symbol, 0.0),
                            minChangeMap.getOrDefault(symbol, 0.0)))
                    .sorted((a, b) -> Double.compare(b.getChangePercent(), a.getChangePercent())) // 按涨跌幅降序
                    .collect(Collectors.toList());

            distribution.add(new DistributionBucket(entry.getKey(), coins.size(), coins, coinDetails));
        }
        data.setDistribution(distribution);

        // 构建所有币种排行榜（按涨跌幅降序）
        List<DistributionBucket.CoinDetail> allCoinsRanking = changeMap.entrySet().stream()
                .map(e -> new DistributionBucket.CoinDetail(
                        e.getKey(),
                        e.getValue(),
                        maxChangeMap.getOrDefault(e.getKey(), 0.0),
                        minChangeMap.getOrDefault(e.getKey(), 0.0)))
                .sorted((a, b) -> Double.compare(b.getChangePercent(), a.getChangePercent()))
                .collect(Collectors.toList());
        data.setAllCoinsRanking(allCoinsRanking);

        log.info("分布统计完成: 上涨={}, 下跌={}, 区间大小={}%", upCount, downCount, bucketSize);
        return data;
    }
}
