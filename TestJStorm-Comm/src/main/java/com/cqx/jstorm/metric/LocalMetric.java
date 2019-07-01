package com.cqx.jstorm.metric;

import backtype.storm.task.TopologyContext;
import com.cqx.jstorm.bean.MetricBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * <pre>
 *     本地监控
 *     > 注册
 *     > 监控
 *     > 按时间窗口进行输出监控记录
 * </pre>
 *
 * @author chenqixu
 */
public class LocalMetric implements IMetric {
    public static final long TIMER_DELAY = 500;// 500毫秒后执行
    public static final long TIMER_PERIOD = 1 * 1000;//1 * 60 * 1000l;// 间隔默认1分钟
    private static final Logger logger = LoggerFactory.getLogger(LocalMetric.class);
    private static Map<String, MetricBean> metricBeanMap = new HashMap<>();
    private Timer timer;
    private DecimalFormat format = new DecimalFormat("###0.0");

    public MetricBean registerTaskCount(String name, MetricType metricType) {
        MetricBean metricBean = new MetricBean(name, metricType);
        metricBeanMap.put(name, metricBean);
        return metricBean;
    }

    public void start() {
        timer = new Timer(true);
        timer.schedule(new Print(), TIMER_DELAY, TIMER_PERIOD);
    }

    public long countAll() {
        long count = 0;
        for (Map.Entry<String, MetricBean> entry : metricBeanMap.entrySet()) {
            count += entry.getValue().getFilenum();
        }
        return count;
    }

    public long count(String name) {
        return metricBeanMap.get(name).getFilenum();
    }

    public void clean() {
        for (Map.Entry<String, MetricBean> entry : metricBeanMap.entrySet()) {
            entry.getValue().clean();
        }
    }

    @Override
    public void initMetric(TopologyContext context) {
        start();
    }

    @Override
    public void registerCounter(String name) {
        registerTaskCount(name, MetricType.INC);
    }

    @Override
    public void registerCounterSize(String name) {
        registerTaskCount(name, MetricType.SIZE);
    }

    @Override
    public void counterInc(String name) {
        metricBeanMap.get(name).Increment(1);
    }

    @Override
    public void counterSize(String name, long value) {
        metricBeanMap.get(name).Increment(value);
    }

    @Override
    public void registerHistogram(String name) {

    }

    @Override
    public void histogramStart(String name) {

    }

    @Override
    public void histogramEnd(String name) {

    }

    private String sumSize(long size) {
        StringBuffer bytes = new StringBuffer();
        if (size >= 1024 * 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0 * 1024.0));
            bytes.append(format.format(i)).append("GB");
        } else if (size >= 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0));
            bytes.append(format.format(i)).append("MB");
        } else if (size >= 1024) {
            double i = (size / (1024.0));
            bytes.append(format.format(i)).append("KB");
        } else if (size < 1024) {
            if (size <= 0) {
                bytes.append("0B");
            } else {
                bytes.append((int) size).append("B");
            }
        }
        return bytes.toString();
    }

    private String sumSizeAvg(long size, int cnt) {
        return format.format(size / (1024.0 * 1024.0) / cnt) + "MB";
    }

    class Print extends TimerTask {
        private int cnt = 1;

        @Override
        public void run() {
            for (Map.Entry<String, MetricBean> entry : metricBeanMap.entrySet()) {
                String metricName = entry.getKey();
                MetricBean metricBean = entry.getValue();
                long count = count(metricName);
                if (metricBean.getMetricType() == MetricType.SIZE) {
                    logger.info("metricName：{}，count：{}，avg：{}", metricName, sumSize(count), sumSizeAvg(count, cnt));
                    cnt++;
                } else {
                    logger.info("metricName：{}，count：{}", metricName, count);
                }
            }
        }
    }
}
