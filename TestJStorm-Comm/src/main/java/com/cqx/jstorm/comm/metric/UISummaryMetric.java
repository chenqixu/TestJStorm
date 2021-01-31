package com.cqx.jstorm.comm.metric;

import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricDef;

/**
 * UISummaryMetric
 *
 * @author chenqixu
 */
public class UISummaryMetric extends UIBasicMetric {

    public static final String[] HEAD = {MetricDef.FAILED_NUM, MetricDef.EMMITTED_NUM, MetricDef.ACKED_NUM, MetricDef.SEND_TPS,
            MetricDef.RECV_TPS, MetricDef.PROCESS_LATENCY};

    public void setMetricValue(MetricSnapshot snapshot, String metricName) {
        if (metricName.equals(MetricDef.MEMORY_USED)) {
            String value = (long) snapshot.get_doubleValue() + "";
            setValue(metricName, value);

        } else {
            String value = UIMetricUtils.getMetricValue(snapshot);
            setValue(metricName, value);
        }
    }
}
