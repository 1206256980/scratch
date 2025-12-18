package com.binance.index.service;

import com.binance.index.dto.DistributionBucket;
import com.binance.index.dto.DistributionData;
import com.binance.index.dto.KlineData;
import com.binance.index.entity.CoinPrice;
import com.binance.index.entity.MarketIndex;
import com.binance.index.repository.CoinPriceRepository;
import com.binance.index.repository.JdbcCoinPriceRepository;
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
    private final CoinPriceRepository coinPriceRepository;
    private final JdbcCoinPriceRepository jdbcCoinPriceRepository;
    private final ExecutorService executorService;

    // 缓存各币种的基准价格（回补起始时间的价格）
    private Map<String, Double> basePrices = new HashMap<>();
    private LocalDateTime basePriceTime;

    public IndexCalculatorService(BinanceApiService binanceApiService,
            MarketIndexRepository marketIndexRepository,
            CoinPriceRepository coinPriceRepository,
            JdbcCoinPriceRepository jdbcCoinPriceRepository,
            ExecutorService klineExecutorService) {
        this.binanceApiService = binanceApiService;
        this.marketIndexRepository = marketIndexRepository;
        this.coinPriceRepository = coinPriceRepository;
        this.jdbcCoinPriceRepository = jdbcCoinPriceRepository;
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

        // 保存每个币种的OHLC价格
        List<CoinPrice> coinPrices = allKlines.stream()
                .filter(k -> k.getClosePrice() > 0)
                .map(k -> new CoinPrice(k.getSymbol(), alignedTime, 
                        k.getOpenPrice(), k.getHighPrice(), k.getLowPrice(), k.getClosePrice()))
                .collect(Collectors.toList());
        jdbcCoinPriceRepository.batchInsert(coinPrices);
        log.debug("保存 {} 个币种价格", coinPrices.size());

        log.info("保存指数: 时间={}, 值={}%, 涨/跌={}/{}, ADR={}, 币种数={}",
                alignedTime, String.format("%.4f", indexValue),
                upCount, downCount, String.format("%.2f", adr), validCount);
        return index;
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

        // 更新全局基准价格
        if (!historicalBasePrices.isEmpty()) {
            basePrices = new HashMap<>(historicalBasePrices);
            basePriceTime = LocalDateTime.now();
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

            // 每5万条批量保存一次（JDBC批量插入更高效）
            if (allCoinPrices.size() >= 50000) {
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
        LocalDateTime baseTime = LocalDateTime.now().minusMinutes(minutes);

        // 从数据库获取最新价格
        List<CoinPrice> latestPrices = coinPriceRepository.findLatestPrices();
        if (latestPrices.isEmpty()) {
            log.warn("数据库中没有价格数据，可能需要等待回补完成");
            return null;
        }

        // 从数据库获取基准时间的价格
        List<CoinPrice> basePriceList = coinPriceRepository.findEarliestPricesAfter(baseTime);
        if (basePriceList.isEmpty()) {
            log.warn("找不到基准时间 {} 的价格数据", baseTime);
            return null;
        }

        // 转换为Map便于查找
        Map<String, Double> currentPriceMap = latestPrices.stream()
                .collect(Collectors.toMap(CoinPrice::getSymbol, CoinPrice::getPrice, (a, b) -> a));
        Map<String, Double> basePriceMap = basePriceList.stream()
                .collect(Collectors.toMap(CoinPrice::getSymbol, CoinPrice::getPrice, (a, b) -> a));

        // 获取时间区间内的最高/最低价格
        LocalDateTime latestTime = latestPrices.get(0).getTimestamp();
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
