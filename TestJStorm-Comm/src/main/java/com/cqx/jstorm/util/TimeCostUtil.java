package com.cqx.jstorm.util;

/**
 * TimeCostUtil
 *
 * @author chenqixu
 */
public class TimeCostUtil {
    long start;
    long end;
    long lastCheckTime = System.currentTimeMillis();

    public void start() {
        start = System.currentTimeMillis();
    }

    public void end() {
        end = System.currentTimeMillis();
    }

    public boolean tag(long limitTime) {
        if (System.currentTimeMillis() - lastCheckTime > limitTime) {
            lastCheckTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    /**
     * 花费的时间
     *
     * @return
     */
    public long getCost() {
        return end - start;
    }
}