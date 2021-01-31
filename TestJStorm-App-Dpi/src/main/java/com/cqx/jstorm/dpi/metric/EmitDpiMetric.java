package com.cqx.jstorm.dpi.metric;

import com.cqx.jstorm.dpi.bean.DpiMetricBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * EmitDpiMetric
 *
 * @author chenqixu
 */
public class EmitDpiMetric {

    public static final long TIMER_DELAY = 500l;// 500毫秒后执行
    public static final long TIMER_PERIOD = 500;//1 * 60 * 1000l;// 间隔默认1分钟
    private static final Logger logger = LoggerFactory.getLogger(EmitDpiMetric.class);
    private static Map<String, DpiMetricBean> dpiMetricBeanMap = new HashMap<>();
    private Timer timer;

    public static DpiMetricBean registerTaskCount(String name) {
        DpiMetricBean dpiMetricBean = new DpiMetricBean(name);
        dpiMetricBeanMap.put(name, dpiMetricBean);
        return dpiMetricBean;
    }

    public void start() {
        timer = new Timer(true);
        timer.schedule(new Print(), TIMER_DELAY, TIMER_PERIOD);
    }

    public long count() {
        long count = 0;
        for (Map.Entry<String, DpiMetricBean> entry : dpiMetricBeanMap.entrySet()) {
            count += entry.getValue().getFilenum();
        }
        return count;
    }

    public void clean() {
        for (Map.Entry<String, DpiMetricBean> entry : dpiMetricBeanMap.entrySet()) {
            entry.getValue().clean();
        }
    }

    class Print extends TimerTask {
        private int cnt = 1;

        @Override
        public void run() {
            long count = count();
            logger.info("count：{}，cnt：{}，avg：{}", count, cnt, count / cnt);
            cnt++;
            if (cnt % 5 == 0) {
                logger.info("clean.");
                clean();
                cnt = 1;
            }
        }
    }
}
