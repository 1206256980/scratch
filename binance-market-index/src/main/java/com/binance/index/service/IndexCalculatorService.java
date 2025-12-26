package com.binance.index.service;

import com.binance.index.dto.DistributionBucket;
import com.binance.index.dto.DistributionData;
import com.binance.index.dto.KlineData;
import com.binance.index.dto.UptrendData;
import com.binance.index.entity.BasePrice;
import com.binance.index.entity.CoinPrice;
import com.binance.index.entity.MarketIndex;
import com.binance.index.repository.CoinPriceRepository;
import com.binance.index.repository.JdbcCoinPriceRepository;
import com.binance.index.repository.BasePriceRepository;
import com.binance.index.repository.MarketIndexRepository;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    // 单边上行数据缓存（5分钟过期，最多缓存10个不同参数的结果）
    private final Cache<String, UptrendData> uptrendCache = Caffeine.newBuilder()
            .maximumSize(10)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();

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
     * 清空单边上行缓存（在新数据采集后调用）
     */
    public void invalidateUptrendCache() {
        uptrendCache.invalidateAll();
        log.debug("单边上行缓存已清空");
    }

    /**
     * 清理下架币种的基准价格（只删除基准价格，保留历史数据用于涨幅分布和单边分析）
     * 当币种重新上架时，会被当作新币处理，重新设置基准价格
     * 
     * @param activeSymbols 当前正在交易的币种列表（从exchangeInfo获取）
     */
    @org.springframework.transaction.annotation.Transactional
    public void cleanupDelistedCoins(Set<String> activeSymbols) {
        if (activeSymbols == null || activeSymbols.isEmpty()) {
            return;
        }

        // 获取数据库中已存储的所有币种基准价格
        Set<String> storedSymbols = basePriceRepository.findAll()
                .stream()
                .map(BasePrice::getSymbol)
                .collect(Collectors.toSet());

        // 求差集：数据库有但API没返回的 = 已下架
        Set<String> delistedSymbols = new HashSet<>(storedSymbols);
        delistedSymbols.removeAll(activeSymbols);

        if (delistedSymbols.isEmpty()) {
            return;
        }

        log.warn("检测到 {} 个币种已下架，清理基准价格（保留历史数据）: {}", delistedSymbols.size(), delistedSymbols);

        for (String symbol : delistedSymbols) {
            // 只删除基准价格（不删除历史价格，保留用于涨幅分布和单边分析）
            basePriceRepository.deleteBySymbol(symbol);

            // 清理内存缓存
            basePrices.remove(symbol);

            log.warn("已清理下架币种 {} 的基准价格（历史数据已保留），重新上架时将重新设置基准价格", symbol);
        }

        log.warn("下架币种基准价格清理完成，共清理 {} 个币种", delistedSymbols.size());
    }

    /**
     * 计算并保存当前时刻的市场指数（实时采集用）
     * 使用并发获取K线数据，获取准确的5分钟成交额
     */
    @org.springframework.transaction.annotation.Transactional
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

        // 清理已下架币种的数据（对比当前活跃币种和数据库中的币种）
        cleanupDelistedCoins(new HashSet<>(symbols));

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

        // 新数据采集后清空单边上行缓存
        invalidateUptrendCache();

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

        // 查询数据库最晚时间点
        LocalDateTime dbLatest = coinPriceRepository.findLatestTimestamp();

        List<String> symbols = binanceApiService.getAllUsdtSymbols();
        long now = System.currentTimeMillis();

        // 计算最新闭合K线时间：对齐到5分钟边界再减5分钟
        // 例如：当前 14:22 → 对齐到14:20 → 减5分钟 → 14:15（这是最新闭合K线的openTime）
        long fiveMinutesMs = 5 * 60 * 1000;
        long alignedNow = (now / fiveMinutesMs) * fiveMinutesMs;
        long latestClosedKlineMs = alignedNow - fiveMinutesMs;
        LocalDateTime latestClosedKline = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(latestClosedKlineMs), ZoneId.of("UTC"));

        long startTime;
        long alignedEndTime = latestClosedKlineMs;

        if (dbLatest == null) {
            // 数据库为空，回补 N 天
            startTime = latestClosedKlineMs - (long) days * 24 * 60 * 60 * 1000;
            log.info("数据库为空，回补 {} 天数据", days);
        } else if (!dbLatest.isBefore(latestClosedKline)) {
            // 数据库已是最新，跳过回补
            log.info("数据库已是最新（dbLatest={}, latestClosedKline={}），跳过API回补", dbLatest, latestClosedKline);
            return;
        } else {
            // 增量回补：从 dbLatest + 5min 开始
            LocalDateTime incrementalStart = dbLatest.plusMinutes(5);
            startTime = incrementalStart.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            log.info("增量回补模式：从 {} 到 {} (dbLatest={})", incrementalStart, latestClosedKline, dbLatest);
        }

        log.info("回补时间范围: {} -> {} (最新闭合K线)",
                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(startTime), ZoneId.of("UTC")),
                latestClosedKline);

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
        boolean basePricesWereLoaded = !existingBasePrices.isEmpty();

        if (!basePricesWereLoaded && !historicalBasePrices.isEmpty()) {
            // 场景1：数据库没有基准价格（首次运行），使用本次回补数据初始化
            basePrices = new HashMap<>(historicalBasePrices);
            basePriceTime = LocalDateTime.now();

            List<BasePrice> basePriceList = historicalBasePrices.entrySet().stream()
                    .map(e -> new BasePrice(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
            basePriceRepository.saveAll(basePriceList);
            log.info("基准价格已初始化并保存到数据库，共 {} 个币种", basePriceList.size());
        } else if (basePricesWereLoaded) {
            // 场景2：数据库有基准价格，检查是否有新币需要添加
            Set<String> newSymbols = new HashSet<>(historicalBasePrices.keySet());
            newSymbols.removeAll(basePrices.keySet());

            if (!newSymbols.isEmpty()) {
                // 将新币的基准价格添加到内存缓存
                for (String symbol : newSymbols) {
                    basePrices.put(symbol, historicalBasePrices.get(symbol));
                }

                // 保存新币基准价格到数据库
                List<BasePrice> newBasePriceList = newSymbols.stream()
                        .map(s -> new BasePrice(s, historicalBasePrices.get(s)))
                        .collect(Collectors.toList());
                basePriceRepository.saveAll(newBasePriceList);
                log.info("新币基准价格已保存: {} 个, 币种={}", newSymbols.size(), newSymbols);
            }
            log.info("使用数据库中的基准价格，共 {} 个币种（含新增 {} 个）", basePrices.size(), newSymbols.size());
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
                    ZoneId.of("UTC"));

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
                Double basePrice = basePrices.get(symbol);

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
            // 使用方法开头已查询的 dbLatest
            LocalDateTime dbEarliest = coinPriceRepository.findEarliestTimestamp();
            log.info("数据库已有价格数据，最早: {}，最晚: {}，本次回补范围内有 {} 个时间点将跳过",
                    dbEarliest, dbLatest, existingPriceTimestamps.size());
        }
        log.info("开始保存币种价格历史...");
        List<CoinPrice> allCoinPrices = new ArrayList<>();
        for (Map.Entry<Long, Map<String, KlineData>> entry : timeSeriesData.entrySet()) {
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(entry.getKey()),
                    ZoneId.of("UTC"));

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
     * 获取指定时间之后最早的价格数据（用于debug接口）
     */
    public List<CoinPrice> getEarliestPricesAfter(LocalDateTime startTime) {
        return coinPriceRepository.findEarliestPricesAfter(startTime);
    }

    /**
     * 获取指定时间之前最晚的价格数据（用于debug接口）
     */
    public List<CoinPrice> getLatestPricesBefore(LocalDateTime endTime) {
        return coinPriceRepository.findLatestPricesBefore(endTime);
    }

    /**
     * 验证指数计算（用于debug接口）
     * 使用全局基准价格和最新数据库价格计算，返回详细信息
     */
    public Map<String, Object> verifyIndexCalculation() {
        Map<String, Object> response = new HashMap<>();

        // 获取全局基准价格
        if (basePrices == null || basePrices.isEmpty()) {
            response.put("success", false);
            response.put("message", "全局基准价格未初始化");
            return response;
        }

        // 获取基准价格创建时间
        List<BasePrice> dbBasePrices = basePriceRepository.findAll();
        LocalDateTime basePriceCreatedAt = null;
        if (!dbBasePrices.isEmpty()) {
            basePriceCreatedAt = dbBasePrices.get(0).getCreatedAt();
        }

        // 获取数据库最新价格
        List<CoinPrice> latestPrices = coinPriceRepository.findLatestPrices();
        if (latestPrices.isEmpty()) {
            response.put("success", false);
            response.put("message", "数据库中没有最新价格数据");
            return response;
        }

        LocalDateTime latestPriceTime = latestPrices.get(0).getTimestamp();

        response.put("basePriceTime", basePriceCreatedAt != null ? basePriceCreatedAt.toString() : "未知");
        response.put("latestPriceTime", latestPriceTime.toString());
        response.put("basePriceCount", basePrices.size());
        response.put("latestPriceCount", latestPrices.size());

        // 转换最新价格为Map
        Map<String, Double> latestPriceMap = latestPrices.stream()
                .collect(Collectors.toMap(CoinPrice::getSymbol, CoinPrice::getPrice, (a, b) -> a));

        // 计算每个币种的涨跌幅
        List<Map<String, Object>> coinDetails = new ArrayList<>();
        double totalChange = 0;
        int validCount = 0;
        int upCount = 0;
        int downCount = 0;

        for (Map.Entry<String, Double> entry : latestPriceMap.entrySet()) {
            String symbol = entry.getKey();
            Double latestPrice = entry.getValue();
            Double basePrice = basePrices.get(symbol);

            if (basePrice != null && basePrice > 0 && latestPrice != null && latestPrice > 0) {
                double changePercent = (latestPrice - basePrice) / basePrice * 100;

                Map<String, Object> detail = new HashMap<>();
                detail.put("symbol", symbol);
                detail.put("basePrice", basePrice);
                detail.put("latestPrice", latestPrice);
                detail.put("changePercent", Math.round(changePercent * 10000) / 10000.0);

                coinDetails.add(detail);

                totalChange += changePercent;
                validCount++;
                if (changePercent > 0)
                    upCount++;
                else if (changePercent < 0)
                    downCount++;
            }
        }

        // 按涨跌幅排序
        coinDetails.sort((a, b) -> Double.compare(
                (Double) b.get("changePercent"),
                (Double) a.get("changePercent")));

        // 计算指数（简单平均）
        double calculatedIndex = validCount > 0 ? totalChange / validCount : 0;

        // 获取系统存储的最新指数
        MarketIndex storedIndex = marketIndexRepository.findTopByOrderByTimestampDesc().orElse(null);

        response.put("success", true);
        response.put("totalCoins", validCount);
        response.put("upCount", upCount);
        response.put("downCount", downCount);
        response.put("calculatedIndex", Math.round(calculatedIndex * 10000) / 10000.0);

        if (storedIndex != null) {
            response.put("storedIndex", Math.round(storedIndex.getIndexValue() * 10000) / 10000.0);
            response.put("storedIndexTime", storedIndex.getTimestamp().toString());
            response.put("indexMatch", Math.abs(calculatedIndex - storedIndex.getIndexValue()) < 0.0001);
        }

        response.put("coins", coinDetails);

        return response;
    }

    /**
     * 删除指定时间范围内的数据（用于清理污染数据）
     * 
     * @param start 开始时间
     * @param end   结束时间
     * @return 删除结果信息
     */
    @org.springframework.transaction.annotation.Transactional
    public Map<String, Object> deleteDataInRange(LocalDateTime start, LocalDateTime end) {
        Map<String, Object> result = new HashMap<>();

        // 先统计要删除的数量
        long indexCount = marketIndexRepository.countByTimestampBetween(start, end);
        long priceCount = coinPriceRepository.findAllDistinctTimestampsBetween(start, end).size();

        log.info("开始删除数据: {} -> {}", start, end);
        log.info("将删除指数记录: {} 条, 价格时间点: {} 个", indexCount, priceCount);

        // 删除数据
        marketIndexRepository.deleteByTimestampBetween(start, end);
        coinPriceRepository.deleteByTimestampBetween(start, end);

        log.info("数据删除完成");

        result.put("deletedIndexCount", indexCount);
        result.put("deletedPriceTimePoints", priceCount);
        result.put("startTime", start.toString());
        result.put("endTime", end.toString());

        return result;
    }

    /**
     * 删除指定币种的所有数据（包括历史价格和基准价格）
     * 
     * @param symbol 币种符号，如 SOLUSDT
     * @return 删除结果信息
     */
    @org.springframework.transaction.annotation.Transactional
    public Map<String, Object> deleteSymbolData(String symbol) {
        Map<String, Object> result = new HashMap<>();
        result.put("symbol", symbol);

        // 统计要删除的记录数
        long priceCount = coinPriceRepository.countBySymbol(symbol);
        boolean hasBasePrice = basePrices.containsKey(symbol) || basePriceRepository.existsById(symbol);

        // 删除历史价格
        coinPriceRepository.deleteBySymbol(symbol);

        // 删除基准价格
        basePriceRepository.deleteBySymbol(symbol);

        // 清理内存缓存
        basePrices.remove(symbol);

        log.warn("已删除币种 {} 的所有数据: 历史价格 {} 条, 基准价格 {} 条, 已从内存缓存移除",
                symbol, priceCount, hasBasePrice ? 1 : 0);

        result.put("deletedPriceCount", priceCount);
        result.put("deletedBasePrice", hasBasePrice);
        result.put("success", true);

        return result;
    }

    /**
     * 查询缺漏的历史价格时间点（只查询，不修复）
     * 与修复接口 repairMissingPriceData 类似，但只返回缺漏信息不做任何修改
     * 
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @return 缺漏时间点统计（按币种）
     */
    public Map<String, Object> getMissingTimestamps(LocalDateTime startTime, LocalDateTime endTime) {
        Map<String, Object> result = new HashMap<>();

        log.info("查询各币种缺漏时间点: {} ~ {}", startTime, endTime);

        // 确保只检查已闭合的K线（当前对齐时间 - 5分钟）
        LocalDateTime latestClosedKline = alignToFiveMinutes(LocalDateTime.now(java.time.ZoneOffset.UTC)).minusMinutes(5);
        LocalDateTime actualEndTime = endTime.isAfter(latestClosedKline) ? latestClosedKline : endTime;
        
        log.info("实际检查范围: {} ~ {} (最新闭合K线: {})", startTime, actualEndTime, latestClosedKline);

        // 1. 生成应该存在的所有时间点
        List<LocalDateTime> expectedTimestamps = new ArrayList<>();
        LocalDateTime checkTime = alignToFiveMinutes(startTime);
        while (!checkTime.isAfter(actualEndTime)) {
            expectedTimestamps.add(checkTime);
            checkTime = checkTime.plusMinutes(5);
        }
        int expectedPerCoin = expectedTimestamps.size();

        // 2. 获取所有活跃币种
        List<String> activeSymbols = binanceApiService.getAllUsdtSymbols();

        // 3. 检查每个币种的缺漏情况
        List<Map<String, Object>> symbolsMissing = new ArrayList<>();
        int totalMissing = 0;
        int symbolsWithMissing = 0;

        for (String symbol : activeSymbols) {
            // 获取该币种已有的时间戳
            List<CoinPrice> prices = coinPriceRepository.findBySymbolInRangeOrderByTime(
                    symbol, startTime, endTime);
            Set<LocalDateTime> existingSet = prices.stream()
                    .map(p -> p.getTimestamp().truncatedTo(java.time.temporal.ChronoUnit.MINUTES))
                    .collect(Collectors.toSet());

            // 找出缺漏的时间点
            List<String> missing = new ArrayList<>();
            for (LocalDateTime ts : expectedTimestamps) {
                if (!existingSet.contains(ts)) {
                    missing.add(ts.toString());
                }
            }

            if (!missing.isEmpty()) {
                Map<String, Object> symbolInfo = new HashMap<>();
                symbolInfo.put("symbol", symbol);
                symbolInfo.put("existing", existingSet.size());
                symbolInfo.put("missing", missing.size());
                symbolInfo.put("missingTimestamps", missing.size() <= 10 ? missing : missing.subList(0, 10)); // 最多显示10个
                symbolsMissing.add(symbolInfo);
                totalMissing += missing.size();
                symbolsWithMissing++;
            }
        }

        // 4. 返回结果
        result.put("totalSymbols", activeSymbols.size());
        result.put("expectedPerCoin", expectedPerCoin);
        result.put("symbolsWithMissing", symbolsWithMissing);
        result.put("totalMissingRecords", totalMissing);
        result.put("details", symbolsMissing.size() <= 50 ? symbolsMissing : symbolsMissing.subList(0, 50)); // 最多显示50个币种

        log.info("查询完成: {}个币种, {}个有缺漏, 共缺{}条记录",
                activeSymbols.size(), symbolsWithMissing, totalMissing);

        return result;
    }

    /**
     * 修复所有币种的历史价格缺失数据
     * 检测每个币种在指定时间范围内的数据缺口，并从币安API回补
     * 
     * @param startTime 开始时间（可选，为空则使用 days 参数）
     * @param endTime   结束时间（可选，为空则使用当前时间）
     * @param days      检查最近多少天的数据，当 startTime 为空时使用
     * @return 修复结果详情
     */
    @org.springframework.transaction.annotation.Transactional
    public Map<String, Object> repairMissingPriceData(LocalDateTime startTime, LocalDateTime endTime, int days) {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> repairedSymbols = new ArrayList<>();

        // 计算时间范围
        LocalDateTime now = LocalDateTime.now(java.time.ZoneOffset.UTC);
        LocalDateTime actualStartTime = startTime != null ? startTime : now.minusDays(days);
        LocalDateTime actualEndTime = endTime != null ? endTime : alignToFiveMinutes(now).minusMinutes(5);

        log.info("开始检测并修复历史价格缺失数据: {} ~ {}", actualStartTime, actualEndTime);

        // 获取当前活跃的所有币种
        List<String> activeSymbols = binanceApiService.getAllUsdtSymbols();
        if (activeSymbols.isEmpty()) {
            result.put("success", false);
            result.put("message", "无法获取活跃币种列表");
            return result;
        }

        log.info("检查 {} 个活跃币种的历史数据完整性...", activeSymbols.size());

        int totalRepairedRecords = 0;
        int checkedSymbols = 0;

        for (String symbol : activeSymbols) {
            checkedSymbols++;

            // 直接查询该币种在时间范围内已有的数据时间戳
            List<CoinPrice> existingPrices = coinPriceRepository.findBySymbolInRangeOrderByTime(symbol, actualStartTime,
                    actualEndTime);

            // 调试：打印前3个币种的现有时间戳
            if (checkedSymbols <= 3) {
                log.info("[调试] 币种 {} 在范围内有 {} 条数据", symbol, existingPrices.size());
                if (!existingPrices.isEmpty()) {
                    log.info("[调试] 币种 {} 现有时间戳: {}", symbol,
                            existingPrices.stream().limit(5).map(p -> p.getTimestamp().toString())
                                    .collect(Collectors.joining(", ")));
                }
            }

            // 使用 truncatedTo(MINUTES) 去除秒和纳秒，只比较到分钟
            Set<LocalDateTime> existingSet = existingPrices.stream()
                    .map(p -> p.getTimestamp().truncatedTo(java.time.temporal.ChronoUnit.MINUTES))
                    .collect(Collectors.toSet());

            // 清空引用帮助 GC
            existingPrices = null;

            // 找出缺失的时间段
            List<LocalDateTime> missingTimestamps = new ArrayList<>();

            LocalDateTime checkTime = alignToFiveMinutes(actualStartTime);
            while (!checkTime.isAfter(actualEndTime)) {
                if (!existingSet.contains(checkTime)) {
                    missingTimestamps.add(checkTime);
                }
                checkTime = checkTime.plusMinutes(5);
            }

            if (missingTimestamps.isEmpty()) {
                continue;
            }

            // 调试：打印缺失的时间戳
            if (checkedSymbols <= 3) { // 只打印前3个币种
                log.info("[调试] 币种 {} 缺失 {} 个时间点: {}", symbol, missingTimestamps.size(),
                        missingTimestamps.size() <= 10 ? missingTimestamps : missingTimestamps.subList(0, 10) + "...");
            }

            // 找出连续的缺失时间段
            List<long[]> missingRanges = findMissingRanges(missingTimestamps);

            int repairedCount = 0;
            List<String> repairedRanges = new ArrayList<>();

            for (long[] range : missingRanges) {
                try {
                    // 修正：如果开始和结束时间相同，扩展结束时间以确保能获取到K线
                    long startMs = range[0];
                    long endMs = range[1];
                    if (endMs <= startMs) {
                        endMs = startMs + 5 * 60 * 1000; // 加5分钟
                    }

                    // 调试日志
                    if (checkedSymbols <= 3) {
                        log.info("[调试] 币种 {} 请求API: {} -> {}", symbol,
                                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(startMs), ZoneId.of("UTC")),
                                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(endMs), ZoneId.of("UTC")));
                    }

                    // 从币安API获取缺失的K线数据
                    List<KlineData> klines = binanceApiService.getKlinesWithPagination(
                            symbol, "5m", startMs, endMs, 500);

                    // 调试日志
                    if (checkedSymbols <= 3) {
                        log.info("[调试] 币种 {} API返回 {} 条K线", symbol, klines.size());
                    }

                    if (!klines.isEmpty()) {
                        // 保存到数据库
                        List<CoinPrice> coinPrices = klines.stream()
                                .filter(k -> k.getClosePrice() > 0)
                                .map(k -> new CoinPrice(k.getSymbol(), k.getTimestamp(),
                                        k.getOpenPrice(), k.getHighPrice(), k.getLowPrice(), k.getClosePrice()))
                                .collect(Collectors.toList());

                        if (!coinPrices.isEmpty()) {
                            jdbcCoinPriceRepository.batchInsert(coinPrices);
                            repairedCount += coinPrices.size();

                            LocalDateTime rangeStart = LocalDateTime.ofInstant(
                                    java.time.Instant.ofEpochMilli(range[0]), ZoneId.of("UTC"));
                            LocalDateTime rangeEnd = LocalDateTime.ofInstant(
                                    java.time.Instant.ofEpochMilli(range[1]), ZoneId.of("UTC"));
                            repairedRanges.add(rangeStart + " ~ " + rangeEnd);
                        }
                    }
                } catch (Exception e) {
                    log.warn("修复 {} 数据失败: {}", symbol, e.getMessage());
                }
            }

            if (repairedCount > 0) {
                Map<String, Object> symbolInfo = new HashMap<>();
                symbolInfo.put("symbol", symbol);
                symbolInfo.put("repairedCount", repairedCount);
                symbolInfo.put("repairedRanges", repairedRanges);
                repairedSymbols.add(symbolInfo);

                totalRepairedRecords += repairedCount;
                log.info("修复币种 {}: {} 条数据, 时间段: {}", symbol, repairedCount, repairedRanges);
            }

            if (checkedSymbols % 50 == 0) {
                log.info("已检查 {}/{} 个币种...", checkedSymbols, activeSymbols.size());
            }
        }

        log.info("历史数据修复完成，共修复 {} 个币种，{} 条数据", repairedSymbols.size(), totalRepairedRecords);

        result.put("success", true);
        result.put("checkedSymbols", activeSymbols.size());
        result.put("repairedSymbolCount", repairedSymbols.size());
        result.put("totalRepairedRecords", totalRepairedRecords);
        result.put("timeRange", actualStartTime + " ~ " + actualEndTime);
        result.put("repairedDetails", repairedSymbols);

        return result;
    }

    /**
     * 找出连续的缺失时间段，合并为时间范围
     */
    private List<long[]> findMissingRanges(List<LocalDateTime> missingTimestamps) {
        List<long[]> ranges = new ArrayList<>();
        if (missingTimestamps.isEmpty()) {
            return ranges;
        }

        // 排序
        missingTimestamps.sort(LocalDateTime::compareTo);

        LocalDateTime rangeStart = missingTimestamps.get(0);
        LocalDateTime rangeEnd = rangeStart;

        for (int i = 1; i < missingTimestamps.size(); i++) {
            LocalDateTime current = missingTimestamps.get(i);

            // 如果当前时间点与上一个相差超过5分钟，说明是新的时间段
            if (java.time.temporal.ChronoUnit.MINUTES.between(rangeEnd, current) > 5) {
                // 保存当前时间段
                ranges.add(new long[] {
                        rangeStart.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                        rangeEnd.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()
                });
                rangeStart = current;
            }
            rangeEnd = current;
        }

        // 保存最后一个时间段
        ranges.add(new long[] {
                rangeStart.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                rangeEnd.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()
        });

        return ranges;
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
     * 时间对齐到5分钟（向上取整）
     */
    private LocalDateTime alignToFiveMinutesCeil(LocalDateTime time) {
        int minute = time.getMinute();
        int second = time.getSecond();
        int nano = time.getNano();
        // 如果已经是整5分钟，直接返回
        if (minute % 5 == 0 && second == 0 && nano == 0) {
            return time.withSecond(0).withNano(0);
        }
        int alignedMinute = ((minute / 5) + 1) * 5;
        if (alignedMinute >= 60) {
            return time.plusHours(1).withMinute(0).withSecond(0).withNano(0);
        }
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

    /**
     * 获取指定时间范围的涨幅分布数据
     *
     * @param startTime 开始时间（基准价格时间点）
     * @param endTime   结束时间（当前价格时间点）
     * @return 涨幅分布数据
     */
    public DistributionData getDistributionByTimeRange(LocalDateTime startTime, LocalDateTime endTime) {
        // 对齐时间到5分钟边界：统一向下取整
        LocalDateTime alignedStart = alignToFiveMinutes(startTime);
        LocalDateTime alignedEnd = alignToFiveMinutes(endTime);
        log.info("计算涨幅分布，时间范围: {} -> {} (对齐后)", alignedStart, alignedEnd);

        long now = System.currentTimeMillis();

        // 从数据库获取开始时间点（基准）的价格
        List<CoinPrice> basePriceList = coinPriceRepository.findEarliestPricesAfter(alignedStart);
        if (basePriceList.isEmpty()) {
            log.warn("找不到开始时间 {} 附近的价格数据", startTime);
            return null;
        }
        LocalDateTime actualStartTime = basePriceList.get(0).getTimestamp();

        // 从数据库获取结束时间点的价格
        List<CoinPrice> endPriceList = coinPriceRepository.findLatestPricesBefore(alignedEnd);
        if (endPriceList.isEmpty()) {
            log.warn("找不到结束时间 {} 附近的价格数据", alignedEnd);
            return null;
        }
        LocalDateTime actualEndTime = endPriceList.get(0).getTimestamp();

        // 转换为Map便于查找
        // 基准价格使用开盘价
        Map<String, Double> basePriceMap = basePriceList.stream()
                .filter(cp -> cp.getOpenPrice() != null)
                .collect(Collectors.toMap(CoinPrice::getSymbol, CoinPrice::getOpenPrice, (a, b) -> a));
        // 结束价格使用收盘价
        Map<String, Double> endPriceMap = endPriceList.stream()
                .collect(Collectors.toMap(CoinPrice::getSymbol, CoinPrice::getPrice, (a, b) -> a));

        // 获取时间区间内的最高/最低价格
        List<Object[]> maxPricesResult = coinPriceRepository.findMaxPricesBySymbolInRange(actualStartTime,
                actualEndTime);
        List<Object[]> minPricesResult = coinPriceRepository.findMinPricesBySymbolInRange(actualStartTime,
                actualEndTime);

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

        log.info("从数据库获取价格: 基准={} 个, 结束={} 个, 最高={} 个, 最低={} 个",
                basePriceMap.size(), endPriceMap.size(), maxPriceMap.size(), minPriceMap.size());

        // 计算涨跌幅（使用结束价格与基准价格比较）
        Map<String, Double> changeMap = new HashMap<>();
        Map<String, Double> maxChangeMap = new HashMap<>();
        Map<String, Double> minChangeMap = new HashMap<>();

        for (Map.Entry<String, Double> entry : endPriceMap.entrySet()) {
            String symbol = entry.getKey();
            Double endPrice = entry.getValue();
            Double basePrice = basePriceMap.get(symbol);
            Double maxPrice = maxPriceMap.get(symbol);
            Double minPrice = minPriceMap.get(symbol);

            if (basePrice != null && basePrice > 0 && endPrice != null && endPrice > 0) {
                double changePercent = (endPrice - basePrice) / basePrice * 100;
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
            bucketSize = 0.2;
        } else if (range <= 5) {
            bucketSize = 0.5;
        } else if (range <= 20) {
            bucketSize = 1;
        } else if (range <= 50) {
            bucketSize = 2;
        } else {
            bucketSize = 5;
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

            List<DistributionBucket.CoinDetail> coinDetails = coins.stream()
                    .map(symbol -> new DistributionBucket.CoinDetail(
                            symbol,
                            changeMap.getOrDefault(symbol, 0.0),
                            maxChangeMap.getOrDefault(symbol, 0.0),
                            minChangeMap.getOrDefault(symbol, 0.0)))
                    .sorted((a, b) -> Double.compare(b.getChangePercent(), a.getChangePercent()))
                    .collect(Collectors.toList());

            distribution.add(new DistributionBucket(entry.getKey(), coins.size(), coins, coinDetails));
        }
        data.setDistribution(distribution);

        // 构建所有币种排行榜
        List<DistributionBucket.CoinDetail> allCoinsRanking = changeMap.entrySet().stream()
                .map(e -> new DistributionBucket.CoinDetail(
                        e.getKey(),
                        e.getValue(),
                        maxChangeMap.getOrDefault(e.getKey(), 0.0),
                        minChangeMap.getOrDefault(e.getKey(), 0.0)))
                .sorted((a, b) -> Double.compare(b.getChangePercent(), a.getChangePercent()))
                .collect(Collectors.toList());
        data.setAllCoinsRanking(allCoinsRanking);

        log.info("分布统计完成: 时间范围 {} -> {}, 上涨={}, 下跌={}, 区间大小={}%",
                actualStartTime, actualEndTime, upCount, downCount, bucketSize);
        return data;
    }

    /**
     * 获取单边上行涨幅分布数据
     * 
     * @param hours            时间范围（小时）
     * @param keepRatio        保留比率阈值（如0.75表示回吐25%涨幅时结束）
     * @param noNewHighCandles 连续多少根K线未创新高视为横盘结束
     * @param minUptrend       最小涨幅过滤（百分比，如4表示只返回>=4%的波段）
     * @return 单边涨幅分布数据
     */
    public UptrendData getUptrendDistribution(double hours, double keepRatio, int noNewHighCandles, double minUptrend) {
        log.info("计算单边上行涨幅分布，时间范围: {}小时，保留比率: {}，横盘K线数: {}, 最小涨幅: {}%", hours, keepRatio, noNewHighCandles, minUptrend);

        long minutes = (long) (hours * 60);
        LocalDateTime alignedNow = alignToFiveMinutes(LocalDateTime.now());
        LocalDateTime startTime = alignedNow.minusMinutes(minutes);
        LocalDateTime endTime = alignedNow;

        return getUptrendDistributionByTimeRange(startTime, endTime, keepRatio, noNewHighCandles, minUptrend);
    }

    /**
     * 获取指定时间范围的单边上行涨幅分布数据
     * 
     * @param startTime        开始时间
     * @param endTime          结束时间
     * @param keepRatio        保留比率阈值（如0.75表示回吐25%涨幅时结束）
     * @param noNewHighCandles 连续多少根K线未创新高视为横盘结束
     * @param minUptrend       最小涨幅过滤（百分比）
     * @return 单边涨幅分布数据
     */
    public UptrendData getUptrendDistributionByTimeRange(LocalDateTime startTime, LocalDateTime endTime,
            double keepRatio, int noNewHighCandles, double minUptrend) {
        // 对齐时间到5分钟边界：统一向下取整
        LocalDateTime alignedStart = alignToFiveMinutes(startTime);
        LocalDateTime alignedEnd = alignToFiveMinutes(endTime);

        // 生成缓存 key（包含对齐后的时间和所有参数）
        String cacheKey = String.format("%s_%s_%.2f_%d_%.2f",
                alignedStart.toString(), alignedEnd.toString(), keepRatio, noNewHighCandles, minUptrend);

        // 检查缓存
        UptrendData cachedData = uptrendCache.getIfPresent(cacheKey);
        if (cachedData != null) {
            log.info("命中缓存: {}", cacheKey);
            return cachedData;
        }

        log.info("计算单边涨幅分布: {} -> {} (对齐后), 保留比率: {}, 横盘K线数: {}, 最小涨幅: {}%", alignedStart, alignedEnd, keepRatio,
                noNewHighCandles, minUptrend);

        // 【优化】分批查询，避免一次性加载过多数据到内存
        long queryStart = System.currentTimeMillis();

        // Step 1: 先获取时间范围内所有币种列表（只返回字符串，内存占用小）
        List<String> allSymbols = coinPriceRepository.findDistinctSymbolsInRange(alignedStart, alignedEnd);
        if (allSymbols.isEmpty()) {
            log.warn("时间范围内没有数据");
            return null;
        }

        log.info("找到 {} 个币种，开始一次性查询处理...", allSymbols.size());

        // 一次性加载所有数据（牺牲内存换取最快速度）
        List<CoinPrice> allPrices = coinPriceRepository.findAllInRangeOrderBySymbolAndTime(alignedStart, alignedEnd);

        if (allPrices.isEmpty()) {
            log.warn("时间范围内没有数据");
            return null;
        }

        // 按币种分组
        Map<String, List<CoinPrice>> pricesBySymbol = allPrices.stream()
                .collect(java.util.stream.Collectors.groupingBy(CoinPrice::getSymbol));

        log.info("查询完成，共 {} 条数据，{} 个币种，开始并行计算...", allPrices.size(), pricesBySymbol.size());

        // 清空原始列表引用，帮助 GC
        allPrices = null;

        // 使用并行流处理
        List<UptrendData.CoinUptrend> allWaves = pricesBySymbol.entrySet().parallelStream()
                .flatMap(entry -> calculateSymbolAllWavesFromData(entry.getKey(), entry.getValue(), keepRatio,
                        noNewHighCandles, minUptrend).stream())
                .collect(java.util.stream.Collectors.toList());

        // 清空分组数据
        pricesBySymbol = null;

        long queryTime = System.currentTimeMillis() - queryStart;
        log.info("一次性查询处理完成，耗时 {}ms，共 {} 个波段", queryTime, allWaves.size());

        // 统计进行中的波段数
        int ongoingCount = (int) allWaves.stream().filter(UptrendData.CoinUptrend::isOngoing).count();

        if (allWaves.isEmpty()) {
            log.warn("没有有效的单边涨幅数据");
            return null;
        }

        // 按单边涨幅降序排序
        allWaves.sort((a, b) -> Double.compare(b.getUptrendPercent(), a.getUptrendPercent()));

        // 计算统计信息
        double totalUptrend = allWaves.stream().mapToDouble(UptrendData.CoinUptrend::getUptrendPercent).sum();
        double avgUptrend = totalUptrend / allWaves.size();
        double maxUptrendValue = allWaves.get(0).getUptrendPercent();
        double minUptrendValue = allWaves.stream().mapToDouble(UptrendData.CoinUptrend::getUptrendPercent).min()
                .orElse(0);

        // 根据数据范围动态确定区间大小（与涨幅分布一致）
        double range = maxUptrendValue - minUptrendValue;
        double bucketSize;
        if (range <= 2) {
            bucketSize = 0.2; // 0.2%区间，适合极小范围
        } else if (range <= 5) {
            bucketSize = 0.5; // 0.5%区间
        } else if (range <= 20) {
            bucketSize = 1; // 1%区间
        } else if (range <= 50) {
            bucketSize = 2; // 2%区间
        } else {
            bucketSize = 5; // 5%区间，适合大范围
        }

        // 计算区间边界
        double bucketMin = Math.floor(minUptrendValue / bucketSize) * bucketSize;
        double bucketMax = Math.ceil(maxUptrendValue / bucketSize) * bucketSize;

        // 创建区间
        Map<String, List<UptrendData.CoinUptrend>> bucketMap = new LinkedHashMap<>();
        for (double start = bucketMin; start < bucketMax; start += bucketSize) {
            String rangeKey;
            if (bucketSize < 1) {
                rangeKey = String.format("%.1f%%~%.1f%%", start, start + bucketSize);
            } else {
                rangeKey = String.format("%.0f%%~%.0f%%", start, start + bucketSize);
            }
            bucketMap.put(rangeKey, new ArrayList<>());
        }

        // 分配波段到区间
        for (UptrendData.CoinUptrend wave : allWaves) {
            double pct = wave.getUptrendPercent();
            double bucketStart = Math.floor(pct / bucketSize) * bucketSize;
            String rangeKey;
            if (bucketSize < 1) {
                rangeKey = String.format("%.1f%%~%.1f%%", bucketStart, bucketStart + bucketSize);
            } else {
                rangeKey = String.format("%.0f%%~%.0f%%", bucketStart, bucketStart + bucketSize);
            }
            if (bucketMap.containsKey(rangeKey)) {
                bucketMap.get(rangeKey).add(wave);
            }
        }

        List<UptrendData.UptrendBucket> distribution = new ArrayList<>();
        for (Map.Entry<String, List<UptrendData.CoinUptrend>> entry : bucketMap.entrySet()) {
            List<UptrendData.CoinUptrend> waves = entry.getValue();
            int bucketOngoing = (int) waves.stream().filter(UptrendData.CoinUptrend::isOngoing).count();
            distribution.add(new UptrendData.UptrendBucket(entry.getKey(), waves.size(), bucketOngoing, waves));
        }

        // 构建响应
        UptrendData data = new UptrendData();
        data.setTimestamp(System.currentTimeMillis());
        data.setTotalCoins(allWaves.size()); // 现在是波段总数
        data.setPullbackThreshold(keepRatio);
        data.setAvgUptrend(Math.round(avgUptrend * 100) / 100.0);
        data.setMaxUptrend(Math.round(maxUptrendValue * 100) / 100.0);
        data.setOngoingCount(ongoingCount);
        data.setDistribution(distribution);
        data.setAllCoinsRanking(allWaves);

        log.info("单边涨幅分布计算完成: 总波段数={}, 进行中={}, 平均涨幅={}%, 最大涨幅={}%",
                allWaves.size(), ongoingCount, data.getAvgUptrend(), data.getMaxUptrend());

        // 存入缓存
        uptrendCache.put(cacheKey, data);
        log.debug("结果已缓存: {}", cacheKey);

        return data;
    }

    /**
     * 计算单个币种的所有符合条件的单边涨幅波段（使用已加载的数据）
     * 
     * 【优化版本】直接使用传入的价格数据，避免数据库查询
     * 
     * @param symbol           币种
     * @param prices           该币种的K线数据列表（已按时间升序排列）
     * @param keepRatio        保留比率阈值（如0.75表示回吐25%涨幅时结束）
     * @param noNewHighCandles 连续多少根K线未创新高视为横盘结束
     * @param minUptrend       最小涨幅过滤（百分比），低于此值的波段不返回
     * @return 该币种的所有符合条件的单边涨幅波段列表
     */
    private List<UptrendData.CoinUptrend> calculateSymbolAllWavesFromData(String symbol, List<CoinPrice> prices,
            double keepRatio, int noNewHighCandles, double minUptrend) {
        if (prices == null || prices.size() < 2) {
            return Collections.emptyList();
        }

        List<UptrendData.CoinUptrend> waves = new ArrayList<>();

        // 波段跟踪变量
        double waveStartPrice = 0; // 波段起点价格（使用低价作为真正起点）
        double wavePeakPrice = 0; // 波段最高价
        double waveLowestLow = 0; // 波段期间的历史最低价（用于判断是否真正破位）
        LocalDateTime waveStartTime = null;
        LocalDateTime wavePeakTime = null;
        int candlesSinceNewHigh = 0; // 连续未创新高的K线数

        // 当前波段状态
        boolean inWave = false;

        for (CoinPrice price : prices) {
            double highPrice = price.getHighPrice() != null ? price.getHighPrice() : price.getPrice();
            double lowPrice = price.getLowPrice() != null ? price.getLowPrice() : price.getPrice();
            double closePrice = price.getPrice();
            LocalDateTime timestamp = price.getTimestamp();

            if (!inWave) {
                // 开始新波段：使用低价作为起点
                waveStartPrice = lowPrice;
                waveStartTime = timestamp;
                waveLowestLow = lowPrice;
                wavePeakPrice = highPrice;
                wavePeakTime = timestamp;
                candlesSinceNewHigh = 0;
                inWave = true;
            } else {
                // 检查是否创新高
                boolean madeNewHigh = false;
                if (highPrice > wavePeakPrice) {
                    wavePeakPrice = highPrice;
                    wavePeakTime = timestamp;
                    candlesSinceNewHigh = 0; // 重置计数器
                    madeNewHigh = true;
                } else {
                    candlesSinceNewHigh++; // 未创新高，计数+1
                }

                // 检查是否创新低（使用低价判断是否真正破位）
                // 只有当K线低价跌破波段历史最低价时，才重置波段起点
                if (lowPrice < waveLowestLow) {
                    // 真正破位，重置波段起点
                    waveStartPrice = lowPrice;
                    waveStartTime = timestamp;
                    waveLowestLow = lowPrice;
                    wavePeakPrice = highPrice;
                    wavePeakTime = timestamp;
                    candlesSinceNewHigh = 0;
                    continue; // 继续下一根K线
                }

                // 计算位置比率
                double range = wavePeakPrice - waveStartPrice;
                double positionRatio = 1.0;
                if (range > 0) {
                    positionRatio = (closePrice - waveStartPrice) / range;
                }

                // 波段结束条件：位置比率 < keepRatio 或 连续N根K线未创新高
                boolean positionTrigger = !madeNewHigh && positionRatio < keepRatio && range > 0;
                boolean sidewaysTrigger = candlesSinceNewHigh >= noNewHighCandles;

                if (positionTrigger || sidewaysTrigger) {
                    // 波段结束，计算涨幅
                    double uptrendPercent = waveStartPrice > 0
                            ? (wavePeakPrice - waveStartPrice) / waveStartPrice * 100
                            : 0;

                    // 符合条件的波段加入列表
                    boolean isDifferentCandle = !waveStartTime.equals(wavePeakTime);
                    if (uptrendPercent >= minUptrend && isDifferentCandle) {
                        waves.add(new UptrendData.CoinUptrend(
                                symbol,
                                Math.round(uptrendPercent * 100) / 100.0,
                                false, // 已结束的波段
                                waveStartTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                                wavePeakTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                                waveStartPrice,
                                wavePeakPrice));
                    }

                    // 回溯找到从波段峰值到当前的最低点作为新波段起点
                    int peakIndex = prices.indexOf(price); // 当前索引
                    double lowestPrice = lowPrice; // 使用当前K线的低价作为初始值
                    LocalDateTime lowestTime = timestamp;

                    // 从峰值时间点往后找最低点（使用低价而非收盘价）
                    for (int j = peakIndex; j >= 0; j--) {
                        CoinPrice p = prices.get(j);
                        if (p.getTimestamp().isBefore(wavePeakTime) || p.getTimestamp().equals(wavePeakTime)) {
                            break;
                        }
                        double pLow = p.getLowPrice() != null ? p.getLowPrice() : p.getPrice();
                        if (pLow < lowestPrice) {
                            lowestPrice = pLow;
                            lowestTime = p.getTimestamp();
                        }
                    }

                    // 以找到的最低点开始新波段
                    waveStartPrice = lowestPrice;
                    waveStartTime = lowestTime;
                    waveLowestLow = lowestPrice; // 重置历史最低价！
                    wavePeakPrice = highPrice;
                    wavePeakTime = timestamp;
                    candlesSinceNewHigh = 0;
                }
            }
        }

        // 处理最后一个波段（进行中的波段）
        if (inWave && waveStartPrice > 0 && wavePeakPrice > waveStartPrice) {
            double uptrendPercent = (wavePeakPrice - waveStartPrice) / waveStartPrice * 100;

            boolean isDifferentCandle = !waveStartTime.equals(wavePeakTime);
            // 最后波段：检查是否还处于"有效上涨"状态
            boolean stillValid = candlesSinceNewHigh < noNewHighCandles;

            if (uptrendPercent >= minUptrend && isDifferentCandle) {
                waves.add(new UptrendData.CoinUptrend(
                        symbol,
                        Math.round(uptrendPercent * 100) / 100.0,
                        stillValid,
                        waveStartTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                        wavePeakTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                        waveStartPrice,
                        wavePeakPrice));
            }
        }

        return waves;
    }

    /**
     * 计算单个币种的所有符合条件的单边涨幅波段
     * 
     * 新算法：使用位置比率法 + 横盘检测
     * - 位置比率 = (当前价 - 起点) / (最高价 - 起点)
     * - 当位置比率 < keepRatio 或 连续N根K线未创新高，波段结束
     * 
     * @param symbol           币种
     * @param startTime        开始时间
     * @param endTime          结束时间
     * @param keepRatio        保留比率阈值（如0.75表示回吐25%涨幅时结束）
     * @param noNewHighCandles 连续多少根K线未创新高视为横盘结束
     * @param minUptrend       最小涨幅过滤（百分比），低于此值的波段不返回
     * @return 该币种的所有符合条件的单边涨幅波段列表
     */
    private List<UptrendData.CoinUptrend> calculateSymbolAllWaves(String symbol, LocalDateTime startTime,
            LocalDateTime endTime, double keepRatio, int noNewHighCandles, double minUptrend) {
        // 获取该币种的所有K线数据
        List<CoinPrice> prices = coinPriceRepository.findBySymbolInRangeOrderByTime(symbol, startTime, endTime);
        if (prices == null || prices.size() < 2) {
            return Collections.emptyList();
        }

        List<UptrendData.CoinUptrend> waves = new ArrayList<>();

        // 波段跟踪变量
        double waveStartPrice = 0; // 波段起点价格（使用低价作为真正起点）
        double wavePeakPrice = 0; // 波段最高价
        double waveLowestLow = 0; // 波段期间的历史最低价（用于判断是否真正破位）
        LocalDateTime waveStartTime = null;
        LocalDateTime wavePeakTime = null;
        int candlesSinceNewHigh = 0; // 连续未创新高的K线数

        // 当前波段状态
        boolean inWave = false;

        for (CoinPrice price : prices) {
            double highPrice = price.getHighPrice() != null ? price.getHighPrice() : price.getPrice();
            double lowPrice = price.getLowPrice() != null ? price.getLowPrice() : price.getPrice();
            double closePrice = price.getPrice();
            LocalDateTime timestamp = price.getTimestamp();

            if (!inWave) {
                // 开始新波段：使用低价作为起点
                waveStartPrice = lowPrice;
                waveStartTime = timestamp;
                waveLowestLow = lowPrice;
                wavePeakPrice = highPrice;
                wavePeakTime = timestamp;
                candlesSinceNewHigh = 0;
                inWave = true;
            } else {
                // 检查是否创新高
                boolean madeNewHigh = false;
                if (highPrice > wavePeakPrice) {
                    wavePeakPrice = highPrice;
                    wavePeakTime = timestamp;
                    candlesSinceNewHigh = 0; // 重置计数器
                    madeNewHigh = true;
                } else {
                    candlesSinceNewHigh++; // 未创新高，计数+1
                }

                // 检查是否创新低（使用低价判断是否真正破位）
                // 只有当K线低价跌破波段历史最低价时，才重置波段起点
                if (lowPrice < waveLowestLow) {
                    // 真正破位，重置波段起点
                    waveStartPrice = lowPrice;
                    waveStartTime = timestamp;
                    waveLowestLow = lowPrice;
                    wavePeakPrice = highPrice;
                    wavePeakTime = timestamp;
                    candlesSinceNewHigh = 0;
                    continue; // 继续下一根K线
                }

                // 计算位置比率
                double range = wavePeakPrice - waveStartPrice;
                double positionRatio = 1.0;
                if (range > 0) {
                    positionRatio = (closePrice - waveStartPrice) / range;
                }

                // 波段结束条件：位置比率 < keepRatio 或 连续N根K线未创新高
                // 注意：刚创新高的K线不触发位置比率结束（因为收盘价总是低于最高价）
                boolean positionTrigger = !madeNewHigh && positionRatio < keepRatio && range > 0;
                boolean sidewaysTrigger = candlesSinceNewHigh >= noNewHighCandles;

                if (positionTrigger || sidewaysTrigger) {
                    // 波段结束，计算涨幅
                    double uptrendPercent = waveStartPrice > 0
                            ? (wavePeakPrice - waveStartPrice) / waveStartPrice * 100
                            : 0;

                    // 符合条件的波段加入列表
                    boolean isDifferentCandle = !waveStartTime.equals(wavePeakTime);
                    if (uptrendPercent >= minUptrend && isDifferentCandle) {
                        waves.add(new UptrendData.CoinUptrend(
                                symbol,
                                Math.round(uptrendPercent * 100) / 100.0,
                                false, // 已结束的波段
                                waveStartTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                                wavePeakTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                                waveStartPrice,
                                wavePeakPrice));
                    }

                    // 回溯找到从波段峰值到当前的最低点作为新波段起点
                    // 这样可以更准确地捕捉真正的涨势起点
                    int peakIndex = prices.indexOf(price); // 当前索引
                    double lowestPrice = lowPrice; // 使用当前K线的低价作为初始值
                    LocalDateTime lowestTime = timestamp;

                    // 从峰值时间点往后找最低点（使用低价而非收盘价）
                    for (int j = peakIndex; j >= 0; j--) {
                        CoinPrice p = prices.get(j);
                        if (p.getTimestamp().isBefore(wavePeakTime) || p.getTimestamp().equals(wavePeakTime)) {
                            break; // 不要回溯到峰值之前
                        }
                        double pLow = p.getLowPrice() != null ? p.getLowPrice() : p.getPrice();
                        if (pLow < lowestPrice) {
                            lowestPrice = pLow;
                            lowestTime = p.getTimestamp();
                        }
                    }

                    // 以找到的最低点开始新波段
                    waveStartPrice = lowestPrice;
                    waveStartTime = lowestTime;
                    waveLowestLow = lowestPrice; // 重置历史最低价！
                    wavePeakPrice = highPrice; // 当前K线的高点
                    wavePeakTime = timestamp;
                    candlesSinceNewHigh = 0;
                }
            }
        }

        // 处理最后一个波段（进行中的波段）
        if (inWave && waveStartPrice > 0 && wavePeakPrice > waveStartPrice) {
            double uptrendPercent = (wavePeakPrice - waveStartPrice) / waveStartPrice * 100;

            boolean isDifferentCandle = !waveStartTime.equals(wavePeakTime);
            // 最后波段：检查是否还处于"有效上涨"状态
            boolean stillValid = candlesSinceNewHigh < noNewHighCandles;

            if (uptrendPercent >= minUptrend && isDifferentCandle) {
                waves.add(new UptrendData.CoinUptrend(
                        symbol,
                        Math.round(uptrendPercent * 100) / 100.0,
                        stillValid, // 如果还没触发横盘结束，标记为进行中
                        waveStartTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                        wavePeakTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli(),
                        waveStartPrice,
                        wavePeakPrice));
            }
        }

        return waves;
    }

    // ==================== V2 优化版本：两阶段并发回补 ====================

    /**
     * 优化版历史数据回补（V2）
     * 
     * 主要优化：
     * 1. 两阶段回补：第一阶段用固定结束时间，第二阶段补充期间新增数据
     * 2. 并发获取：使用 Semaphore 控制并发数
     * 3. 立即保存：每个币种获取后立即保存到数据库（DB 耗时作为自然间隔）
     * 4. limit=99：降低 API 权重消耗（权重 1 vs 原来的 5）
     * 5. 每阶段完成后立即计算指数
     * 
     * @param days        回补天数
     * @param concurrency 并发数（建议 5-10）
     */
    public void backfillHistoricalDataV2(int days, int concurrency) {
        log.info("========== 开始 V2 优化版回补（{}天，并发数{}）==========", days, concurrency);
        long totalStartTime = System.currentTimeMillis();

        // 1. 从数据库加载基准价格
        List<BasePrice> existingBasePrices = basePriceRepository.findAll();
        if (!existingBasePrices.isEmpty()) {
            basePrices = existingBasePrices.stream()
                    .collect(Collectors.toMap(BasePrice::getSymbol, BasePrice::getPrice, (a, b) -> a));
            basePriceTime = existingBasePrices.get(0).getCreatedAt();
            log.info("从数据库加载基准价格成功，共 {} 个币种", basePrices.size());
        } else {
            log.info("数据库中没有基准价格，将从历史数据初始化");
        }

        // 2. 查询数据库最晚时间点（用于增量回补判断）
        LocalDateTime dbLatest = coinPriceRepository.findLatestTimestamp();

        // 3. 计算第一阶段的时间范围（固定结束时间，所有币种共用）
        LocalDateTime now = LocalDateTime.now(java.time.ZoneOffset.UTC);
        LocalDateTime phase1EndTime = alignToFiveMinutes(now).minusMinutes(5); // 最新闭合K线
        LocalDateTime phase1StartTime;

        if (dbLatest == null) {
            // 数据库为空，回补 N 天
            phase1StartTime = phase1EndTime.minusDays(days);
            log.info("数据库为空，全量回补 {} 天", days);
        } else if (!dbLatest.isBefore(phase1EndTime)) {
            // 数据库已是最新，跳过回补
            log.info("数据库已是最新（dbLatest={}, latestClosed={}），跳过API回补", dbLatest, phase1EndTime);
            return;
        } else {
            // 增量回补：从 dbLatest + 5min 开始
            phase1StartTime = dbLatest.plusMinutes(5);
            log.info("增量回补模式：从 {} 到 {} (dbLatest={})", phase1StartTime, phase1EndTime, dbLatest);
        }

        // ==================== 第一阶段：主回补 ====================
        log.info("========== 第一阶段：主回补 ==========");
        log.info("时间范围: {} -> {} (固定)", phase1StartTime, phase1EndTime);

        Map<String, Double> phase1BasePrices = backfillPhaseV2(phase1StartTime, phase1EndTime, concurrency,
                existingBasePrices.isEmpty());

        // 更新基准价格（如果是首次运行）
        if (existingBasePrices.isEmpty() && !phase1BasePrices.isEmpty()) {
            basePrices = new HashMap<>(phase1BasePrices);
            basePriceTime = LocalDateTime.now();
            List<BasePrice> basePriceList = phase1BasePrices.entrySet().stream()
                    .map(e -> new BasePrice(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
            basePriceRepository.saveAll(basePriceList);
            log.info("基准价格已初始化并保存，共 {} 个币种", basePriceList.size());
        } else if (!existingBasePrices.isEmpty() && !phase1BasePrices.isEmpty()) {
            // 检查是否有新币需要添加
            Set<String> newSymbols = new HashSet<>(phase1BasePrices.keySet());
            newSymbols.removeAll(basePrices.keySet());
            if (!newSymbols.isEmpty()) {
                for (String symbol : newSymbols) {
                    basePrices.put(symbol, phase1BasePrices.get(symbol));
                }
                List<BasePrice> newBasePriceList = newSymbols.stream()
                        .map(s -> new BasePrice(s, phase1BasePrices.get(s)))
                        .collect(Collectors.toList());
                basePriceRepository.saveAll(newBasePriceList);
                log.info("新币基准价格已保存: {} 个", newSymbols.size());
            }
        }

        // 第一阶段：计算指数
        log.info("第一阶段：计算指数...");
        calculateAndSaveIndexesForRange(phase1StartTime, phase1EndTime);

        // ==================== 第二阶段：增量回补 ====================
        log.info("========== 第二阶段：增量回补 ==========");

        LocalDateTime phase2StartTime = phase1EndTime.plusMinutes(5);
        LocalDateTime phase2EndTime = alignToFiveMinutes(LocalDateTime.now(java.time.ZoneOffset.UTC)).minusMinutes(5);

        if (!phase2StartTime.isAfter(phase2EndTime)) {
            log.info("增量范围: {} -> {}", phase2StartTime, phase2EndTime);
            backfillPhaseV2(phase2StartTime, phase2EndTime, concurrency, false);

            // 第二阶段：计算指数
            log.info("第二阶段：计算指数...");
            calculateAndSaveIndexesForRange(phase2StartTime, phase2EndTime);
        } else {
            log.info("无需增量回补，数据已是最新");
        }

        long totalElapsed = System.currentTimeMillis() - totalStartTime;
        log.info("========== V2 回补全部完成！总耗时: {}ms ({}分钟) ==========",
                totalElapsed, totalElapsed / 60000);
    }

    /**
     * 执行单个阶段的并发回补（方案B：每次 API 调用后立即保存）
     * 
     * 关键改进：不使用 getKlinesWithPagination，而是自己控制每次 API 调用后立即保存
     * 这样 DB 操作的耗时成为自然的 API 间隔，避免被限流
     * 
     * @param startTime         开始时间
     * @param endTime           结束时间
     * @param concurrency       并发数
     * @param collectBasePrices 是否收集基准价格（首次运行时需要）
     * @return 收集到的基准价格（每个币种最早的 openPrice）
     */
    private Map<String, Double> backfillPhaseV2(LocalDateTime startTime, LocalDateTime endTime,
            int concurrency, boolean collectBasePrices) {

        List<String> symbols = binanceApiService.getAllUsdtSymbols();
        if (symbols.isEmpty()) {
            log.warn("无法获取交易对列表");
            return Collections.emptyMap();
        }

        long startMs = startTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long endMs = endTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();

        // 批量查询已存在的时间戳（优化）
        Set<LocalDateTime> existingTimestamps = new HashSet<>(
                coinPriceRepository.findAllDistinctTimestampsBetween(startTime, endTime));
        log.info("本阶段已存在 {} 个时间点将跳过", existingTimestamps.size());

        Semaphore semaphore = new Semaphore(concurrency);
        AtomicInteger completed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        AtomicInteger totalApiCalls = new AtomicInteger(0);
        AtomicInteger totalSaved = new AtomicInteger(0);

        // 用于收集新币的基准价格（线程安全）
        Map<String, Double> collectedBasePrices = new java.util.concurrent.ConcurrentHashMap<>();

        long phaseStartTime = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = symbols.stream()
                .map(symbol -> CompletableFuture.runAsync(() -> {
                    try {
                        semaphore.acquire();

                        // 分批获取并立即保存（方案B核心逻辑）
                        long currentStart = startMs;
                        boolean isFirstBatch = true;

                        while (currentStart <= endMs) {
                            // 检查是否被限流
                            if (binanceApiService.isRateLimited()) {
                                log.warn("检测到限流，停止回补 {}", symbol);
                                break;
                            }

                            // 获取一批 K 线（limit=500，权重5）
                            List<KlineData> batch = binanceApiService.getKlines(
                                    symbol, "5m", currentStart, endMs, 500);

                            totalApiCalls.incrementAndGet();

                            if (batch.isEmpty()) {
                                break;
                            }

                            // 收集基准价格（使用第一批的第一条）
                            if (collectBasePrices && isFirstBatch && !batch.isEmpty()) {
                                collectedBasePrices.putIfAbsent(symbol, batch.get(0).getOpenPrice());
                                isFirstBatch = false;
                            }

                            // 立即过滤并保存这一批（方案B：每次API调用后立即保存）
                            List<CoinPrice> pricesToSave = new ArrayList<>();
                            for (KlineData kline : batch) {
                                LocalDateTime timestamp = kline.getTimestamp();
                                if (!existingTimestamps.contains(timestamp) && kline.getClosePrice() > 0) {
                                    pricesToSave.add(new CoinPrice(
                                            kline.getSymbol(), timestamp,
                                            kline.getOpenPrice(), kline.getHighPrice(),
                                            kline.getLowPrice(), kline.getClosePrice(),
                                            kline.getVolume())); // 成交额
                                }
                            }

                            // 立即保存
                            if (!pricesToSave.isEmpty()) {
                                jdbcCoinPriceRepository.batchInsert(pricesToSave);
                                totalSaved.addAndGet(pricesToSave.size());
                            }

                            // 使用配置的请求间隔，确保不超过速率限制
                            long intervalMs = binanceApiService.getRequestIntervalMs();
                            if (intervalMs > 0) {
                                try {
                                    Thread.sleep(intervalMs);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }

                            // 计算下一批的起始时间
                            KlineData lastKline = batch.get(batch.size() - 1);
                            long lastTime = lastKline.getTimestamp()
                                    .atZone(ZoneId.of("UTC"))
                                    .toInstant()
                                    .toEpochMilli();
                            currentStart = lastTime + 300000; // +5分钟
                        }

                        int done = completed.incrementAndGet();
                        if (done % 50 == 0 || done == symbols.size()) {
                            long elapsed = System.currentTimeMillis() - phaseStartTime;
                            log.info("回补进度: {}/{} (API调用:{}, 已保存:{}条) 耗时:{}s",
                                    done, symbols.size(), totalApiCalls.get(), totalSaved.get(), elapsed / 1000);
                        }

                    } catch (Exception e) {
                        int failCount = failed.incrementAndGet();
                        log.error("回补失败 {}: {}", symbol, e.getMessage());

                        // 连续失败多次时等待
                        if (failCount % 10 == 0) {
                            log.warn("连续失败 {} 次，等待5秒后继续...", failCount);
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    } finally {
                        semaphore.release();
                    }
                }, executorService))
                .collect(Collectors.toList());

        // 等待所有任务完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long phaseElapsed = System.currentTimeMillis() - phaseStartTime;
        log.info("本阶段完成: 成功={}, 跳过={}, 失败={}, API调用={}, 保存={}条, 耗时={}s",
                completed.get(), skipped.get(), failed.get(), totalApiCalls.get(), totalSaved.get(),
                phaseElapsed / 1000);

        return collectedBasePrices;
    }

    /**
     * 计算并保存指定时间范围内的所有指数
     * 
     * @param startTime 开始时间
     * @param endTime   结束时间
     */
    private void calculateAndSaveIndexesForRange(LocalDateTime startTime, LocalDateTime endTime) {
        // 批量查询已存在的指数时间戳
        Set<LocalDateTime> existingIndexTimestamps = new HashSet<>(
                marketIndexRepository.findAllTimestampsBetween(startTime, endTime));

        // 获取所有需要计算的时间点
        List<LocalDateTime> timestamps = coinPriceRepository.findAllDistinctTimestampsBetween(startTime, endTime);
        log.info("时间范围内有 {} 个时间点，已有指数 {} 个", timestamps.size(), existingIndexTimestamps.size());

        List<MarketIndex> indexList = new ArrayList<>();

        for (LocalDateTime timestamp : timestamps) {
            // 跳过已存在的
            if (existingIndexTimestamps.contains(timestamp)) {
                continue;
            }

            // 获取该时间点的所有币种价格
            List<CoinPrice> prices = coinPriceRepository.findByTimestamp(timestamp);

            double totalChange = 0;
            double totalVolume = 0;
            int validCount = 0;
            int upCount = 0;
            int downCount = 0;

            for (CoinPrice price : prices) {
                String symbol = price.getSymbol();
                Double basePrice = basePrices.get(symbol);

                if (basePrice == null || basePrice <= 0) {
                    continue;
                }

                double changePercent = (price.getPrice() - basePrice) / basePrice * 100;

                // 累加成交额
                if (price.getVolume() != null) {
                    totalVolume += price.getVolume();
                }

                if (changePercent > 0) {
                    upCount++;
                } else if (changePercent < 0) {
                    downCount++;
                }

                totalChange += changePercent;
                validCount++;
            }

            if (validCount > 0) {
                double indexValue = totalChange / validCount;
                double adr = downCount > 0 ? (double) upCount / downCount : upCount;
                indexList.add(new MarketIndex(timestamp, indexValue, totalVolume, validCount, upCount, downCount, adr));
            }
        }

        // 批量保存
        if (!indexList.isEmpty()) {
            marketIndexRepository.saveAll(indexList);
            log.info("指数计算完成，新增 {} 条记录", indexList.size());
        } else {
            log.info("无新指数需要保存");
        }
    }
}
