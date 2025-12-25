package com.binance.index.scheduler;

import com.binance.index.service.IndexCalculatorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DataCollectorScheduler {

    private static final Logger log = LoggerFactory.getLogger(DataCollectorScheduler.class);

    private final IndexCalculatorService indexCalculatorService;

    @Value("${index.backfill.days}")
    private int backfillDays;

    @Value("${binance.api.backfill-concurrency:5}")
    private int backfillConcurrency;

    private volatile boolean isBackfillComplete = false;

    public DataCollectorScheduler(IndexCalculatorService indexCalculatorService) {
        this.indexCalculatorService = indexCalculatorService;
    }

    /**
     * 应用启动后执行历史数据回补（使用 V2 优化版）
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("应用启动完成，开始 V2 优化版回补历史数据...");

        // 异步执行回补，不阻塞应用启动
        new Thread(() -> {
            try {
                // 设置回补状态
                indexCalculatorService.setBackfillInProgress(true);

                // 使用 V2 优化版回补（两阶段并发）
                indexCalculatorService.backfillHistoricalDataV2(backfillDays, backfillConcurrency);

                // V2 版本不需要 flushPendingData，因为每阶段结束后已计算指数

                // 清除回补状态
                indexCalculatorService.setBackfillInProgress(false);
                isBackfillComplete = true;
                log.info("V2 历史数据回补完成");
            } catch (Exception e) {
                log.error("历史数据回补失败: {}", e.getMessage(), e);
                indexCalculatorService.setBackfillInProgress(false);
            }
        }, "backfill-v2-thread").start();
    }

    /**
     * 每5分钟采集一次数据
     * cron: 秒 分 时 日 月 周
     * 10 0/5 * * * * 表示每5分钟的第10秒执行（给币安API时间更新K线数据）
     */
    @Scheduled(cron = "10 0/5 * * * *")
    public void collectData() {
        // if (!isBackfillComplete) {
        //     // 回补未完成时，采集数据但暂存到内存队列，不保存到数据库
        //     // 这样可以确保回补期间的实时数据不会丢失
        //     log.info("回补进行中，采集数据暂存到内存队列");
        //     try {
        //         indexCalculatorService.collectAndBuffer();
        //     } catch (Exception e) {
        //         log.error("暂存采集失败: {}", e.getMessage(), e);
        //     }
        //     return;
        // }
        if (!isBackfillComplete) {
            log.debug("历史数据回补尚未完成，跳过本次采集");
            return;
        }

        try {
            indexCalculatorService.calculateAndSaveCurrentIndex();
        } catch (Exception e) {
            log.error("数据采集失败: {}", e.getMessage(), e);
        }
    }

    // 基准价格永不自动刷新，仅在首次启动时通过回补历史数据设定
    // 如需更改回补天数，修改配置 index.backfill.days (默认7天)
}
