package com.cqx.jstorm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * TimeUtil
 *
 * @author chenqixu
 */
public class TimeUtil {
    protected static final Logger logger = LoggerFactory.getLogger(TimeUtil.class);
    protected static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 时间比较
     *
     * <pre>
     * 	time1小于time2 -1
     * 	time1等于time2 0
     * 	time1大于time2 1
     * </pre>
     *
     * @param time1
     * @param time2
     * @return 文件周期
     */
    public static int timeComparison(String time1, String time2) {
        if (time1 != null && time2 != null) {
            // 高并发情况下必须同步simpleDateFormat，具体参考SimpleDateFormat类说明
            synchronized (simpleDateFormat) {
                try {
                    long l1 = simpleDateFormat.parse(time1).getTime();
                    long l2 = simpleDateFormat.parse(time2).getTime();
                    return (l1 - l2 > 0) ? 1 : (l1 - l2 < 0 ? -1 : 0);
                } catch (ParseException e) {
                    logger.error("★★★ 时间比较异常，时间1 {}，时间2 {}", time1, time2);
                    logger.error("★★★ 具体错误信息：" + e.getMessage(), e);
                }
            }
        }
        return 2;
    }
}
