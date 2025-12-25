package com.binance.index.controller;

import com.binance.index.dto.DistributionData;
import com.binance.index.dto.IndexDataPoint;
import com.binance.index.dto.UptrendData;
import com.binance.index.entity.MarketIndex;
import com.binance.index.service.IndexCalculatorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
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
     * 支持两种模式：
     * 1. 相对时间模式: hours=24 (表示从24小时前到现在)
     * 2. 绝对时间模式: start=2024-12-12 10:05&end=2024-12-12 10:15 (指定时间范围)
     * 
     * 绝对时间模式优先级更高，如果同时传了start/end和hours，使用绝对时间模式
     * 
     * @param hours    相对基准时间（多少小时前），支持小数如0.25表示15分钟，默认168小时（7天）
     * @param start    开始时间（基准价格时间），格式: yyyy-MM-dd HH:mm
     * @param end      结束时间（当前价格时间），格式: yyyy-MM-dd HH:mm
     * @param timezone 输入时间的时区，默认 Asia/Shanghai
     */
    @GetMapping("/distribution")
    public ResponseEntity<Map<String, Object>> getDistribution(
            @RequestParam(defaultValue = "168") double hours,
            @RequestParam(required = false) String start,
            @RequestParam(required = false) String end,
            @RequestParam(defaultValue = "Asia/Shanghai") String timezone) {

        Map<String, Object> response = new HashMap<>();

        // 如果提供了 start 和 end，使用绝对时间模式
        if (start != null && end != null && !start.isEmpty() && !end.isEmpty()) {
            try {
                // 解析时间
                java.time.LocalDateTime startLocal = parseDateTime(start);
                java.time.LocalDateTime endLocal = parseDateTime(end);

                // 将输入时间从用户时区转换为UTC
                java.time.ZoneId userZone = java.time.ZoneId.of(timezone);
                java.time.ZoneId utcZone = java.time.ZoneId.of("UTC");

                java.time.LocalDateTime startUtc = startLocal.atZone(userZone).withZoneSameInstant(utcZone)
                        .toLocalDateTime();
                java.time.LocalDateTime endUtc = endLocal.atZone(userZone).withZoneSameInstant(utcZone)
                        .toLocalDateTime();

                // 验证时间范围
                if (startUtc.isAfter(endUtc)) {
                    response.put("success", false);
                    response.put("message", "开始时间不能晚于结束时间");
                    return ResponseEntity.badRequest().body(response);
                }

                DistributionData data = indexCalculatorService.getDistributionByTimeRange(startUtc, endUtc);

                if (data != null) {
                    response.put("success", true);
                    response.put("mode", "timeRange");
                    response.put("inputTimezone", timezone);
                    response.put("inputStart", start);
                    response.put("inputEnd", end);
                    response.put("utcStart", startUtc.toString());
                    response.put("utcEnd", endUtc.toString());
                    response.put("data", data);
                } else {
                    response.put("success", false);
                    response.put("message", "获取分布数据失败，可能指定时间范围内无数据");
                }

                return ResponseEntity.ok(response);

            } catch (Exception e) {
                response.put("success", false);
                response.put("message", "时间格式错误，请使用格式: yyyy-MM-dd HH:mm");
                response.put("error", e.getMessage());
                return ResponseEntity.badRequest().body(response);
            }
        }

        // 否则使用相对时间模式（原逻辑）
        DistributionData data = indexCalculatorService.getDistribution(hours);

        if (data != null) {
            response.put("success", true);
            response.put("mode", "hours");
            response.put("hours", hours);
            response.put("data", data);
        } else {
            response.put("success", false);
            response.put("message", "获取分布数据失败");
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 获取单边上行涨幅分布数据
     * 
     * 使用位置比率法 + 横盘检测：
     * - 位置比率 = (当前价 - 起点) / (最高价 - 起点)
     * - 当位置比率 < keepRatio 或 连续N根K线未创新高，波段结束
     * 
     * @param hours             相对基准时间（多少小时前），默认168小时（7天）
     * @param keepRatio         保留比率阈值（如0.75表示回吐25%涨幅时结束），默认0.75
     * @param noNewHighCandles  连续多少根K线未创新高视为横盘结束，默认6
     * @param minUptrend        最小涨幅过滤（百分比），默认4%，低于此值的波段不返回
     * @param start             开始时间，格式: yyyy-MM-dd HH:mm
     * @param end               结束时间，格式: yyyy-MM-dd HH:mm
     * @param timezone          输入时间的时区，默认 Asia/Shanghai
     */
    @GetMapping("/uptrend-distribution")
    public ResponseEntity<Map<String, Object>> getUptrendDistribution(
            @RequestParam(defaultValue = "168") double hours,
            @RequestParam(defaultValue = "0.75") double keepRatio,
            @RequestParam(defaultValue = "6") int noNewHighCandles,
            @RequestParam(defaultValue = "4") double minUptrend,
            @RequestParam(required = false) String start,
            @RequestParam(required = false) String end,
            @RequestParam(defaultValue = "Asia/Shanghai") String timezone) {

        Map<String, Object> response = new HashMap<>();

        // 如果提供了 start 和 end，使用绝对时间模式
        if (start != null && end != null && !start.isEmpty() && !end.isEmpty()) {
            try {
                java.time.LocalDateTime startLocal = parseDateTime(start);
                java.time.LocalDateTime endLocal = parseDateTime(end);

                java.time.ZoneId userZone = java.time.ZoneId.of(timezone);
                java.time.ZoneId utcZone = java.time.ZoneId.of("UTC");

                java.time.LocalDateTime startUtc = startLocal.atZone(userZone).withZoneSameInstant(utcZone)
                        .toLocalDateTime();
                java.time.LocalDateTime endUtc = endLocal.atZone(userZone).withZoneSameInstant(utcZone)
                        .toLocalDateTime();

                if (startUtc.isAfter(endUtc)) {
                    response.put("success", false);
                    response.put("message", "开始时间不能晚于结束时间");
                    return ResponseEntity.badRequest().body(response);
                }

                UptrendData data = indexCalculatorService.getUptrendDistributionByTimeRange(startUtc, endUtc, keepRatio, noNewHighCandles, minUptrend);

                if (data != null) {
                    response.put("success", true);
                    response.put("mode", "timeRange");
                    response.put("keepRatio", keepRatio);
                    response.put("noNewHighCandles", noNewHighCandles);
                    response.put("minUptrend", minUptrend);
                    response.put("inputTimezone", timezone);
                    response.put("inputStart", start);
                    response.put("inputEnd", end);
                    response.put("data", data);
                } else {
                    response.put("success", false);
                    response.put("message", "获取单边涨幅数据失败，可能指定时间范围内无数据或无符合条件的波段");
                }

                return ResponseEntity.ok(response);

            } catch (Exception e) {
                response.put("success", false);
                response.put("message", "时间格式错误，请使用格式: yyyy-MM-dd HH:mm");
                response.put("error", e.getMessage());
                return ResponseEntity.badRequest().body(response);
            }
        }

        // 否则使用相对时间模式
        UptrendData data = indexCalculatorService.getUptrendDistribution(hours, keepRatio, noNewHighCandles, minUptrend);

        if (data != null) {
            response.put("success", true);
            response.put("mode", "hours");
            response.put("hours", hours);
            response.put("keepRatio", keepRatio);
            response.put("noNewHighCandles", noNewHighCandles);
            response.put("minUptrend", minUptrend);
            response.put("data", data);
        } else {
            response.put("success", false);
            response.put("message", "获取单边涨幅数据失败");
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
            // 添加东八区时间字段
            java.time.ZonedDateTime cnTime = p.getTimestamp().atZone(java.time.ZoneId.of("UTC"))
                    .withZoneSameInstant(java.time.ZoneId.of("Asia/Shanghai"));
            item.put("timestampCN", cnTime.toLocalDateTime().toString());
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
    /**
     * 调试接口：验证指数计算
     * 使用全局基准价格（内存中的basePrices）和数据库最新价格进行计算
     * 无需任何参数，直接验证当前实时指数计算是否正确
     */
    @GetMapping("/debug/verify")
    public ResponseEntity<Map<String, Object>> debugVerifyIndex() {
        Map<String, Object> response = indexCalculatorService.verifyIndexCalculation();
        return ResponseEntity.ok(response);
    }

    /**
     * 删除指定时间范围内的数据（用于清理污染数据）
     * 时间格式: yyyy-MM-dd HH:mm 或 yyyy-MM-ddTHH:mm:ss
     * timezone: 输入时间的时区，默认 Asia/Shanghai（东八区），数据库存的是UTC
     * 示例: DELETE /api/index/data?start=2025-12-21 10:00&end=2025-12-21
     * 10:30&timezone=Asia/Shanghai
     */
    @DeleteMapping("/data")
    public ResponseEntity<Map<String, Object>> deleteDataInRange(
            @RequestParam String start,
            @RequestParam String end,
            @RequestParam(defaultValue = "Asia/Shanghai") String timezone) {

        Map<String, Object> response = new HashMap<>();

        try {
            // 解析时间，支持多种格式
            java.time.LocalDateTime startLocal = parseDateTime(start);
            java.time.LocalDateTime endLocal = parseDateTime(end);

            // 将输入时间从用户时区转换为UTC（数据库存的是UTC）
            java.time.ZoneId userZone = java.time.ZoneId.of(timezone);
            java.time.ZoneId utcZone = java.time.ZoneId.of("UTC");

            java.time.LocalDateTime startUtc = startLocal.atZone(userZone).withZoneSameInstant(utcZone)
                    .toLocalDateTime();
            java.time.LocalDateTime endUtc = endLocal.atZone(userZone).withZoneSameInstant(utcZone).toLocalDateTime();

            // 验证时间范围
            if (startUtc.isAfter(endUtc)) {
                response.put("success", false);
                response.put("message", "开始时间不能晚于结束时间");
                return ResponseEntity.badRequest().body(response);
            }

            // 执行删除
            Map<String, Object> result = indexCalculatorService.deleteDataInRange(startUtc, endUtc);

            response.put("success", true);
            response.put("message", "数据删除成功");
            response.put("inputTimezone", timezone);
            response.put("inputStart", start);
            response.put("inputEnd", end);
            response.put("utcStart", startUtc.toString());
            response.put("utcEnd", endUtc.toString());
            response.putAll(result);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "时间格式错误，请使用格式: yyyy-MM-dd HH:mm 或 yyyy-MM-ddTHH:mm:ss");
            response.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * 删除指定币种的所有数据（包括历史价格和基准价格）
     * 用于手动清理问题币种的数据
     * 
     * 示例: DELETE /api/index/symbol/XXXUSDT
     * 
     * @param symbol 币种符号，如 SOLUSDT
     */
    @DeleteMapping("/symbol/{symbol}")
    public ResponseEntity<Map<String, Object>> deleteSymbolData(@PathVariable String symbol) {
        Map<String, Object> response = new HashMap<>();

        if (symbol == null || symbol.isEmpty()) {
            response.put("success", false);
            response.put("message", "币种符号不能为空");
            return ResponseEntity.badRequest().body(response);
        }

        try {
            Map<String, Object> result = indexCalculatorService.deleteSymbolData(symbol.toUpperCase());
            response.put("success", true);
            response.put("message", "币种数据删除成功");
            response.putAll(result);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "删除失败: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 修复所有币种的历史价格缺失数据
     * 检测每个币种在指定时间范围内的数据缺口，并从币安API回补
     * 
     * 示例: 
     * - POST /api/index/repair?days=7 （修复最近7天）
     * - POST /api/index/repair?start=2024-12-20 00:00&end=2024-12-24 00:00 （指定时间范围）
     * 
     * @param days 检查最近多少天的数据，默认7天（当 start 为空时使用）
     * @param start 开始时间（可选）
     * @param end 结束时间（可选）
     */
    @PostMapping("/repair")
    public ResponseEntity<Map<String, Object>> repairMissingData(
            @RequestParam(defaultValue = "7") int days,
            @RequestParam(required = false) String start,
            @RequestParam(required = false) String end) {
        
        Map<String, Object> response = new HashMap<>();

        // 验证 days 参数（仅当未指定 start 时使用）
        if (start == null && (days <= 0 || days > 60)) {
            response.put("success", false);
            response.put("message", "days 参数必须在 1-60 之间");
            return ResponseEntity.badRequest().body(response);
        }

        try {
            // 解析时间参数（按东八区解析，转换为 UTC）
            java.time.LocalDateTime startTime = null;
            java.time.LocalDateTime endTime = null;
            java.time.ZoneId beijingZone = java.time.ZoneId.of("Asia/Shanghai");
            java.time.ZoneId utcZone = java.time.ZoneId.of("UTC");
            
            if (start != null && !start.isEmpty()) {
                java.time.LocalDateTime beijingTime = parseDateTime(start);
                // 东八区转 UTC（减8小时）
                startTime = beijingTime.atZone(beijingZone).withZoneSameInstant(utcZone).toLocalDateTime();
            }
            if (end != null && !end.isEmpty()) {
                java.time.LocalDateTime beijingTime = parseDateTime(end);
                // 东八区转 UTC（减8小时）
                endTime = beijingTime.atZone(beijingZone).withZoneSameInstant(utcZone).toLocalDateTime();
            }
            
            Map<String, Object> result = indexCalculatorService.repairMissingPriceData(startTime, endTime, days);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "修复失败: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 解析时间字符串，支持多种格式
     */
    private java.time.LocalDateTime parseDateTime(String dateTimeStr) {
        // 尝试多种格式
        String[] patterns = {
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd HH:mm",
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm"
        };

        for (String pattern : patterns) {
            try {
                java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
                return java.time.LocalDateTime.parse(dateTimeStr, formatter);
            } catch (Exception ignored) {
            }
        }

        // 尝试 ISO 格式
        return java.time.LocalDateTime.parse(dateTimeStr);
    }
}
