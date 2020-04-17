package com.cqx.jstorm.metric;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

/**
 * UIMetricUtils
 *
 * @author chenqixu
 */
public class UIMetricUtils {

    public static final DecimalFormat format = new DecimalFormat(",###.##");
    private static final Logger LOG = LoggerFactory.getLogger(UIMetricUtils.class);

    /**
     * get MetricSnapshot formatted value string
     */
    public static String getMetricValue(MetricSnapshot snapshot) {
        if (snapshot == null) return null;
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return format(snapshot.get_longValue());
            case GAUGE:
                return format(snapshot.get_doubleValue());
            case METER:
                return format(snapshot.get_m1());
            case HISTOGRAM:
                return format(snapshot.get_mean());
            default:
                return "0";
        }
    }

    public static String format(double value) {
        return format.format(value);
    }

    public static String format(double value, String f) {
        DecimalFormat _format = new DecimalFormat(f);
        return _format.format(value);
    }

    public static String format(long value) {
        return format.format(value);
    }

    public static UISummaryMetric getSummaryMetrics(List<MetricInfo> infos, int window) {
        if (infos == null || infos.size() == 0) {
            return null;
        }
        MetricInfo info = infos.get(infos.size() - 1);
        return getSummaryMetrics(info, window);
    }

    public static UISummaryMetric getSummaryMetrics(MetricInfo info, int window) {
        UISummaryMetric summaryMetric = new UISummaryMetric();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String metricName = UIMetricUtils.extractMetricName(split_name);

                if (!metric.getValue().containsKey(window)) {
                    LOG.debug("snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                summaryMetric.setMetricValue(snapshot, metricName);
            }
        }
        return summaryMetric;
    }

    // Extract MetricName from 'CC@SequenceTest4-1-1439469823@Merge@0@@sys@Emitted',which is 'Emitted'
    public static String extractMetricName(String[] strs) {
        if (strs.length < 6) return null;
        return strs[strs.length - 1];
    }
}
