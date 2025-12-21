package com.binance.index.controller;

import com.binance.index.dto.DistributionData;
import com.binance.index.dto.IndexDataPoint;
import com.binance.index.entity.MarketIndex;
import com.binance.index.service.IndexCalculatorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/index")
public class IndexController {

    private final IndexCalculatorService indexCalculatorService;

    public IndexController(IndexCalculatorService indexCalculatorService) {
        this.indexCalculatorService = indexCalculatorService;
    }

    /**
     * 获取当前市场指数
     */
    @GetMapping("/current")
    public ResponseEntity<Map<String, Object>> getCurrentIndex() {
        MarketIndex latest = indexCalculatorService.getLatestIndex();

        Map<String, Object> response = new HashMap<>();
        if (latest != null) {
            response.put("success", true);
            response.put("data", toDataPoint(latest));
        } else {
            response.put("success", false);
            response.put("message", "暂无数据");
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 获取历史指数数据
     * 
     * @param hours 小时数，默认168小时（7天）
     */
    @GetMapping("/history")
    public ResponseEntity<Map<String, Object>> getHistoryData(
            @RequestParam(defaultValue = "168") int hours) {

        List<MarketIndex> historyData = indexCalculatorService.getHistoryData(hours);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("count", historyData.size());
        response.put("data", historyData.stream()
                .map(this::toDataPoint)
                .collect(Collectors.toList()));

        return ResponseEntity.ok(response);
    }

    /**
     * 获取统计信息
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        MarketIndex latest = indexCalculatorService.getLatestIndex();
        List<MarketIndex> last24h = indexCalculatorService.getHistoryData(24);
        List<MarketIndex> last72h = indexCalculatorService.getHistoryData(72);
        List<MarketIndex> last168h = indexCalculatorService.getHistoryData(168);
        List<MarketIndex> last720h = indexCalculatorService.getHistoryData(720);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);

        Map<String, Object> stats = new HashMap<>();

        // 当前值
        if (latest != null) {
            stats.put("current", latest.getIndexValue());
            stats.put("coinCount", latest.getCoinCount());
            // 返回毫秒时间戳，前端可以直接用 new Date() 解析
            stats.put("lastUpdate", latest.getTimestamp()
                    .atZone(java.time.ZoneOffset.UTC)
                    .toInstant()
                    .toEpochMilli());
        }

        // 24小时变化
        if (!last24h.isEmpty() && last24h.size() > 1) {
            double first = last24h.get(0).getIndexValue();
            double last = last24h.get(last24h.size() - 1).getIndexValue();
            stats.put("change24h", last - first);

            // 24小时最高最低
            double max24h = last24h.stream().mapToDouble(MarketIndex::getIndexValue).max().orElse(0);
            double min24h = last24h.stream().mapToDouble(MarketIndex::getIndexValue).min().orElse(0);
            stats.put("high24h", max24h);
            stats.put("low24h", min24h);
        }

        // 3天变化
        if (!last72h.isEmpty() && last72h.size() > 1) {
            double first = last72h.get(0).getIndexValue();
            double last = last72h.get(last72h.size() - 1).getIndexValue();
            stats.put("change3d", last - first);

            // 3天最高最低
            double max3d = last72h.stream().mapToDouble(MarketIndex::getIndexValue).max().orElse(0);
            double min3d = last72h.stream().mapToDouble(MarketIndex::getIndexValue).min().orElse(0);
            stats.put("high3d", max3d);
            stats.put("low3d", min3d);
        }

        // 7天变化
        if (!last168h.isEmpty() && last168h.size() > 1) {
            double first = last168h.get(0).getIndexValue();
            double last = last168h.get(last168h.size() - 1).getIndexValue();
            stats.put("change7d", last - first);

            // 7天最高最低
            double max7d = last168h.stream().mapToDouble(MarketIndex::getIndexValue).max().orElse(0);
            double min7d = last168h.stream().mapToDouble(MarketIndex::getIndexValue).min().orElse(0);
            stats.put("high7d", max7d);
            stats.put("low7d", min7d);
        }

        // 30天变化
        if (!last720h.isEmpty() && last720h.size() > 1) {
            double first = last720h.get(0).getIndexValue();
            double last = last720h.get(last720h.size() - 1).getIndexValue();
            stats.put("change30d", last - first);

            // 30天最高最低
            double max30d = last720h.stream().mapToDouble(MarketIndex::getIndexValue).max().orElse(0);
            double min30d = last720h.stream().mapToDouble(MarketIndex::getIndexValue).min().orElse(0);
            stats.put("high30d", max30d);
            stats.put("low30d", min30d);
        }

        response.put("stats", stats);
        return ResponseEntity.ok(response);
    }

    private IndexDataPoint toDataPoint(MarketIndex index) {
        return new IndexDataPoint(
                index.getTimestamp(),
                index.getIndexValue(),
                index.getTotalVolume(),
                index.getCoinCount(),
                index.getUpCount(),
                index.getDownCount(),
                index.getAdr());
    }

    /**
     * 获取涨幅分布数据
     * 
     * @param hours 基准时间（多少小时前），支持小数如0.25表示15分钟，默认168小时（7天）
     */
    @GetMapping("/distribution")
    public ResponseEntity<Map<String, Object>> getDistribution(
            @RequestParam(defaultValue = "168") double hours) {

        DistributionData data = indexCalculatorService.getDistribution(hours);

        Map<String, Object> response = new HashMap<>();
        if (data != null) {
            response.put("success", true);
            response.put("data", data);
        } else {
            response.put("success", false);
            response.put("message", "获取分布数据失败");
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 调试接口：查询指定币种的历史价格数据
     * 
     * @param symbol 币种符号，如 SOLUSDT
     * @param hours  查询多少小时的数据，默认1小时
     */
    @GetMapping("/debug/prices")
    public ResponseEntity<Map<String, Object>> debugPrices(
            @RequestParam String symbol,
            @RequestParam(defaultValue = "1") double hours) {

        java.time.LocalDateTime startTime = java.time.LocalDateTime.now().minusMinutes((long) (hours * 60));
        List<com.binance.index.entity.CoinPrice> prices = indexCalculatorService.getCoinPriceHistory(symbol, startTime);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("symbol", symbol);
        response.put("queryStartTime", startTime.toString());
        response.put("count", prices.size());
        response.put("data", prices.stream().map(p -> {
            Map<String, Object> item = new HashMap<>();
            item.put("timestamp", p.getTimestamp().toString());
            item.put("openPrice", p.getOpenPrice());
            item.put("highPrice", p.getHighPrice());
            item.put("lowPrice", p.getLowPrice());
            item.put("closePrice", p.getPrice());
            return item;
        }).collect(Collectors.toList()));

        return ResponseEntity.ok(response);
    }

    /**
     * 调试接口：查询所有基准价格
     */
    @GetMapping("/debug/basePrices")
    public ResponseEntity<Map<String, Object>> debugBasePrices() {
        List<com.binance.index.entity.BasePrice> basePrices = indexCalculatorService.getAllBasePrices();

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("count", basePrices.size());

        if (!basePrices.isEmpty()) {
            response.put("createdAt", basePrices.get(0).getCreatedAt().toString());
        }

        response.put("data", basePrices.stream().map(p -> {
            Map<String, Object> item = new HashMap<>();
            item.put("symbol", p.getSymbol());
            item.put("price", p.getPrice());
            item.put("createdAt", p.getCreatedAt().toString());
            return item;
        }).collect(Collectors.toList()));

        return ResponseEntity.ok(response);
    }
}
