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

    private volatile boolean isBackfillComplete = false;

    public DataCollectorScheduler(IndexCalculatorService indexCalculatorService) {
        this.indexCalculatorService = indexCalculatorService;
    }

    /**
     * 应用启动后执行历史数据回补
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("应用启动完成，开始回补历史数据...");

        // 异步执行回补，不阻塞应用启动
        new Thread(() -> {
            try {
                indexCalculatorService.backfillHistoricalData(backfillDays);
                isBackfillComplete = true;
                log.info("历史数据回补完成");
            } catch (Exception e) {
                log.error("历史数据回补失败: {}", e.getMessage(), e);
            }
        }, "backfill-thread").start();
    }

    /**
     * 每5分钟采集一次数据
     * cron: 秒 分 时 日 月 周
     * 0 0/5 * * * * 表示每5分钟的第0秒执行
     */
    @Scheduled(cron = "0 0/5 * * * *")
    public void collectData() {
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
