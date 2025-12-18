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
}
