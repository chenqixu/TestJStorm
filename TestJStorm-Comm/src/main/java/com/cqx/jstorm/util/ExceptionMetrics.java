package com.cqx.jstorm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExceptionMetrics
 *
 * @author chenqixu
 */
public class ExceptionMetrics {

    private final static Logger logger = LoggerFactory.getLogger(ExceptionMetrics.class);
    private final static long timeCycle = 1 * 60 * 1000;
    private final static int errorNum = 2;
    /**
     * 异常抛出次数。 没有用同步对象，此数值不要求精确
     */
    private long exceptionNum = 0;
    private long LastMarkStartTime = 0;// 用来记录时间游标。每1分钟记一次
    private IExceptionDo iExceptionDo;// 异常处理

    public ExceptionMetrics(IExceptionDo iExceptionDo) {
        this.iExceptionDo = iExceptionDo;
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
            iExceptionDo.exceptionDo();
            logger.error(timeCycle + "毫秒内进程捕获的Exception超过" + errorNum + "条，进程退出重启=========" + str + e.getMessage(), e);
            throw new RuntimeException(timeCycle + "毫秒内进程捕获的Exception超过" + errorNum + "条，进程退出重启=========" + str + e.getMessage(), e);
        }
    }

    public void markExceptionSingle(String str, Exception e) {
        logger.error("捕获到组件初始化异常:", e);
        iExceptionDo.exceptionDo();
        throw new RuntimeException("捕获到组件初始化异常，进程退出重启=========" + str + e.getMessage(), e);
    }
}
