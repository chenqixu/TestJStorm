package com.cqx.jstorm.dpi.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 扫描工具类
 *
 * @author chenqixu
 */
public class ScanUtil {
    private static Logger logger = LoggerFactory.getLogger(ScanUtil.class);
    public int condition = 0;
    public long scanInterval = 0;
    private AtomicBoolean scan = new AtomicBoolean(true);

    public ScanUtil(int condition, long scanInterval) {
        this.condition = condition;
        this.scanInterval = scanInterval;
        logger.info("[ScanUtil] condition：{}，scanInterval：{}", condition, scanInterval);
    }

    public void resetIsScan() {
        scan.set(true);
        logger.debug("[ScanUtil] 每隔{}毫秒重置一次扫描机会标志", scanInterval);
    }

    public boolean isScan(int scanQueueSize) {
        // 满足条件 且 一次扫描标志为允许（scanInterval内一次机会）
        if (scanQueueSize <= condition && scan.get()) {
            logger.info("[ScanUtil] 满足条件scanQueueSize {} <= boltNum * MAXQUEUENUM {} 且 一次扫描标志为{}", scanQueueSize, condition, scan.get());
            // 置一次扫描标志为不允许
            scan.set(false);
            return true;
        }
        return false;
    }

    public long getScanInterval() {
        return scanInterval;
    }
}
