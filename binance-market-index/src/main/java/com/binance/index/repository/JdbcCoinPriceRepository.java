package com.binance.index.repository;

import com.binance.index.entity.CoinPrice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

/**
 * 使用 JDBC 批量插入 CoinPrice，绕过 JPA 的逐条插入限制
 * 性能提升：从 30秒/万条 → 1-2秒/万条
 */
@Repository
public class JdbcCoinPriceRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcCoinPriceRepository.class);

    private final JdbcTemplate jdbcTemplate;

    // 每批次插入数量
    private static final int BATCH_SIZE = 5000;

    public JdbcCoinPriceRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 批量插入 CoinPrice 数据
     * 使用 JDBC batch 实现真正的批量插入
     */
    @Transactional
    public void batchInsert(List<CoinPrice> prices) {
        if (prices == null || prices.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        String sql = "INSERT INTO coin_price (symbol, timestamp, price) VALUES (?, ?, ?)";

        // 分批处理，避免内存溢出
        int totalSize = prices.size();
        int insertedCount = 0;

        for (int i = 0; i < totalSize; i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, totalSize);
            List<CoinPrice> batch = prices.subList(i, end);

            jdbcTemplate.batchUpdate(sql, batch, batch.size(), (ps, price) -> {
                ps.setString(1, price.getSymbol());
                ps.setTimestamp(2, Timestamp.valueOf(price.getTimestamp()));
                ps.setDouble(3, price.getPrice());
            });

            insertedCount += batch.size();
            
            if (insertedCount % 50000 == 0 || insertedCount == totalSize) {
                log.info("JDBC批量插入进度: {}/{}", insertedCount, totalSize);
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("JDBC批量插入完成: {} 条记录，耗时 {}ms (平均 {} 条/秒)",
                totalSize, elapsed, elapsed > 0 ? totalSize * 1000 / elapsed : totalSize);
    }
}
