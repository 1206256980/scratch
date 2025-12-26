package com.binance.index.repository;

import com.binance.index.entity.CoinPrice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface CoinPriceRepository extends JpaRepository<CoinPrice, Long> {

        /**
         * 获取指定时间点附近的所有币种价格
         * 查找最接近目标时间且不晚于目标时间的记录
         */
        @Query("SELECT cp FROM CoinPrice cp WHERE cp.timestamp = " +
                        "(SELECT MAX(cp2.timestamp) FROM CoinPrice cp2 WHERE cp2.timestamp <= :targetTime)")
        List<CoinPrice> findPricesAtTime(@Param("targetTime") LocalDateTime targetTime);

        /**
         * 获取指定时间点的价格（精确匹配）
         */
        List<CoinPrice> findByTimestamp(LocalDateTime timestamp);

        /**
         * 获取指定时间范围内最早的时间点的价格
         */
        @Query("SELECT cp FROM CoinPrice cp WHERE cp.timestamp = " +
                        "(SELECT MIN(cp2.timestamp) FROM CoinPrice cp2 WHERE cp2.timestamp >= :startTime)")
        List<CoinPrice> findEarliestPricesAfter(@Param("startTime") LocalDateTime startTime);

        /**
         * 获取指定时间之前最晚的时间点的价格
         */
        @Query("SELECT cp FROM CoinPrice cp WHERE cp.timestamp = " +
                        "(SELECT MAX(cp2.timestamp) FROM CoinPrice cp2 WHERE cp2.timestamp <= :endTime)")
        List<CoinPrice> findLatestPricesBefore(@Param("endTime") LocalDateTime endTime);

        /**
         * 获取最新时间点的所有币种价格
         */
        @Query("SELECT cp FROM CoinPrice cp WHERE cp.timestamp = " +
                        "(SELECT MAX(cp2.timestamp) FROM CoinPrice cp2)")
        List<CoinPrice> findLatestPrices();

        /**
         * 检查某时间点是否已有数据
         */
        boolean existsByTimestamp(LocalDateTime timestamp);

        /**
         * 删除指定时间之前的数据（用于清理旧数据）
         */
        void deleteByTimestampBefore(LocalDateTime timestamp);

        /**
         * 获取时间区间内每个币种的最高价（使用K线最高价）
         */
        @Query("SELECT cp.symbol, MAX(cp.highPrice) FROM CoinPrice cp " +
                        "WHERE cp.timestamp >= :startTime AND cp.timestamp <= :endTime " +
                        "GROUP BY cp.symbol")
        List<Object[]> findMaxPricesBySymbolInRange(@Param("startTime") LocalDateTime startTime,
                        @Param("endTime") LocalDateTime endTime);

        /**
         * 获取时间区间内每个币种的最低价（使用K线最低价）
         */
        @Query("SELECT cp.symbol, MIN(cp.lowPrice) FROM CoinPrice cp " +
                        "WHERE cp.timestamp >= :startTime AND cp.timestamp <= :endTime " +
                        "GROUP BY cp.symbol")
        List<Object[]> findMinPricesBySymbolInRange(@Param("startTime") LocalDateTime startTime,
                        @Param("endTime") LocalDateTime endTime);

        /**
         * 批量查询时间范围内所有已存在的时间戳（去重，用于优化回补时的存在性检查）
         */
        @Query("SELECT DISTINCT cp.timestamp FROM CoinPrice cp WHERE cp.timestamp >= :startTime AND cp.timestamp <= :endTime")
        List<LocalDateTime> findAllDistinctTimestampsBetween(@Param("startTime") LocalDateTime startTime,
                        @Param("endTime") LocalDateTime endTime);

        /**
         * 查询指定币种在时间范围内的所有价格记录（调试用）
         */
        @Query("SELECT cp FROM CoinPrice cp WHERE cp.symbol = :symbol AND cp.timestamp >= :startTime ORDER BY cp.timestamp DESC")
        List<CoinPrice> findBySymbolAndTimeRange(@Param("symbol") String symbol,
                        @Param("startTime") LocalDateTime startTime);

        /**
         * 获取数据库中最早的时间点
         */
        @Query("SELECT MIN(cp.timestamp) FROM CoinPrice cp")
        LocalDateTime findEarliestTimestamp();

        /**
         * 获取数据库中最晚的时间点
         */
        @Query("SELECT MAX(cp.timestamp) FROM CoinPrice cp")
        LocalDateTime findLatestTimestamp();

        /**
         * 删除指定时间范围内的数据
         */
        void deleteByTimestampBetween(LocalDateTime start, LocalDateTime end);

        /**
         * 获取指定币种在时间范围内的所有K线数据（按时间升序，用于单边涨幅计算）
         */
        @Query("SELECT cp FROM CoinPrice cp WHERE cp.symbol = :symbol " +
                        "AND cp.timestamp >= :startTime AND cp.timestamp <= :endTime " +
                        "ORDER BY cp.timestamp ASC")
        List<CoinPrice> findBySymbolInRangeOrderByTime(@Param("symbol") String symbol,
                        @Param("startTime") LocalDateTime startTime,
                        @Param("endTime") LocalDateTime endTime);

        /**
         * 获取时间范围内所有不重复的币种列表
         */
        @Query("SELECT DISTINCT cp.symbol FROM CoinPrice cp " +
                        "WHERE cp.timestamp >= :startTime AND cp.timestamp <= :endTime")
        List<String> findDistinctSymbolsInRange(@Param("startTime") LocalDateTime startTime,
                        @Param("endTime") LocalDateTime endTime);

        /**
         * 批量获取时间范围内所有币种的K线数据（按币种和时间排序，用于单边涨幅计算优化）
         * 一次查询替代多次单币种查询，大幅提升性能
         */
        @Query("SELECT cp FROM CoinPrice cp " +
                        "WHERE cp.timestamp >= :startTime AND cp.timestamp <= :endTime " +
                        "ORDER BY cp.symbol ASC, cp.timestamp ASC")
        List<CoinPrice> findAllInRangeOrderBySymbolAndTime(@Param("startTime") LocalDateTime startTime,
                        @Param("endTime") LocalDateTime endTime);

        /**
         * 按币种列表批量查询时间范围内的K线数据（用于分批处理优化）
         */
        @Query("SELECT cp FROM CoinPrice cp " +
                        "WHERE cp.symbol IN :symbols " +
                        "AND cp.timestamp >= :startTime AND cp.timestamp <= :endTime " +
                        "ORDER BY cp.symbol ASC, cp.timestamp ASC")
        List<CoinPrice> findBySymbolsInRange(@Param("symbols") List<String> symbols,
                        @Param("startTime") LocalDateTime startTime,
                        @Param("endTime") LocalDateTime endTime);

        /**
         * 按币种删除所有历史价格（用于清理下架币种）
         */
        void deleteBySymbol(String symbol);

        /**
         * 统计指定币种的记录数（用于日志输出删除了多少条）
         */
        long countBySymbol(String symbol);

        /**
         * 一次性查询时间范围内所有币种的时间戳（用于修复接口优化）
         * 返回 [symbol, timestamp] 的列表，可按币种分组
         */
        @Query("SELECT cp.symbol, cp.timestamp FROM CoinPrice cp " +
                        "WHERE cp.timestamp >= :startTime AND cp.timestamp <= :endTime")
        List<Object[]> findSymbolTimestampsInRange(@Param("startTime") LocalDateTime startTime,
                        @Param("endTime") LocalDateTime endTime);
}
