package com.cqx.jstorm.util;

import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.spout.ISpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * ExceptionMetrics
 *
 * @author chenqixu
 */
public class ExceptionMetrics {

    private final static Logger logger = LoggerFactory.getLogger(ExceptionMetrics.class);
    private final static long timeCycle = 1 * 60 * 1000;
    private final static int errorNum = 2;
    private static ExceptionMetrics exceptionMetrics = new ExceptionMetrics();
    /**
     * 异常抛出次数。 没有用同步对象，此数值不要求精确
     */
    private long exceptionNum = 0;
    private long LastMarkStartTime = 0;// 用来记录时间游标。每1分钟记一次
    private List<ISpout> iSpoutList = new ArrayList<>();
    private List<IBolt> iBoltList = new ArrayList<>();

    private ExceptionMetrics() {
    }

    public static ExceptionMetrics getInstance() {
        synchronized (exceptionMetrics) {
            if (exceptionMetrics == null)
                synchronized (exceptionMetrics) {
                    exceptionMetrics = new ExceptionMetrics();
                }
        }
        return exceptionMetrics;
    }

    public void registerSpout(ISpout iSpout) {
        logger.info("异常监控ISpout注册：{}", iSpout);
        iSpoutList.add(iSpout);
    }

    public void registerBolt(IBolt iBolt) {
        logger.info("异常监控IBolt注册：{}", iBolt);
        iBoltList.add(iBolt);
    }

    public void release() {
        for (ISpout iSpout : iSpoutList) {
            logger.info("释放ISpout：{}", iSpout);
            iSpout.close();
        }
        for (IBolt iBolt : iBoltList) {
            logger.info("释放IBolt：{}", iBolt);
            iBolt.cleanup();
        }
    }

    public void markException(String str, Exception e) {
        exceptionNum++;
        long lastTime = System.currentTimeMillis() - LastMarkStartTime;
        logger.error("捕获到组件异常:", e);
        // 过了1分钟了，切一下时间片
        if (lastTime > timeCycle) {
            LastMarkStartTime = System.currentTimeMillis();
            exceptionNum = 0;
        }
        if (exceptionNum > errorNum) { // 如果每timeCycle时间超过errorNum个异常，就要报错进程退出
            logger.error(timeCycle + "毫秒内进程捕获的Exception超过" + errorNum + "条，进程退出重启=========" + str + e.getMessage(), e);
            // 释放
            release();
            throw new RuntimeException(timeCycle + "毫秒内进程捕获的Exception超过" + errorNum + "条，进程退出重启=========" + str + e.getMessage(), e);
        }
    }

    public void markExceptionSingle(String str, Exception e) {
        logger.error("捕获到组件初始化异常:", e);
        // 释放
        release();
        throw new RuntimeException("捕获到组件初始化异常，进程退出重启=========" + str + e.getMessage(), e);
    }
}
