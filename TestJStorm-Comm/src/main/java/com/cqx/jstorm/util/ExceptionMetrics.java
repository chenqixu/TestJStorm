package com.cqx.jstorm.util;

import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.spout.ISpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ExceptionMetrics
 *
 * @author chenqixu
 */
public class ExceptionMetrics {

    private final static Logger logger = LoggerFactory.getLogger(ExceptionMetrics.class);
    private final static long timeCycle = 60 * 1000L;
    private final static int errorNum = 2;
    private final static Object lock = new Object();
    //运行时是否抑制异常配置
    private final static String RUNTIME_SUPPRESSION_EXCEPTION = "runtime.suppression.exception";
    private static ExceptionMetrics exceptionMetrics;
    //运行时是否抑制异常
    private boolean exceptionIsSuppression = false;
    /**
     * 异常抛出次数。 没有用同步对象，此数值不要求精确
     */
    private long exceptionNum = 0;
    private long LastMarkStartTime = 0;// 用来记录时间游标。每1分钟记一次
    private List<ISpout> iSpoutList = new ArrayList<>();
    private List<IBolt> iBoltList = new ArrayList<>();

    private ExceptionMetrics() {
    }

    private ExceptionMetrics(boolean exceptionIsSuppression) {
        this.exceptionIsSuppression = exceptionIsSuppression;
        logger.info("运行时是否抑制异常：{}", exceptionIsSuppression);
    }

    public static ExceptionMetrics getInstance(Map stormConf) {
        synchronized (lock) {
            if (exceptionMetrics == null)
                synchronized (lock) {
                    if (stormConf != null) {
                        Object param = stormConf.get(RUNTIME_SUPPRESSION_EXCEPTION);
                        if (param != null && param instanceof Boolean) {
                            exceptionMetrics = new ExceptionMetrics((Boolean) param);
                        } else {
                            exceptionMetrics = new ExceptionMetrics();
                        }
                    } else {
                        exceptionMetrics = new ExceptionMetrics();
                    }
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
        logger.error("捕获到组件异常:" + e.getMessage(), e);
        // 过了1分钟了，切一下时间片
        if (lastTime > timeCycle) {
            LastMarkStartTime = System.currentTimeMillis();
            exceptionNum = 0;
        }
        // 如果每timeCycle时间超过errorNum个异常，就要报错进程退出
        if ((exceptionNum > errorNum) && !exceptionIsSuppression) {
            logger.error(timeCycle + "毫秒内进程捕获的Exception超过" + errorNum + "条，进程退出重启=========" + str + e.getMessage(), e);
            // 释放
            release();
            throw new RuntimeException(timeCycle + "毫秒内进程捕获的Exception超过" + errorNum + "条，进程退出重启=========" + str + e.getMessage(), e);
        }
    }

    public void markExceptionSingle(String str, Exception e) {
        logger.error("捕获到组件初始化异常:" + e.getMessage(), e);
        // 释放
        release();
        throw new RuntimeException("捕获到组件初始化异常，进程退出重启=========" + str + e.getMessage(), e);
    }
}
