package com.cqx.jstorm.metric;

import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricDef;

import java.util.HashMap;
import java.util.Map;

/**
 * UIBasicMetric
 *
 * @author chenqixu
 */
public class UIBasicMetric {

    // <metricName, value>
    protected Map<String, String> metrics = new HashMap<>();

    public static final String[] HEAD = {MetricDef.EMMITTED_NUM, MetricDef.ACKED_NUM, MetricDef.FAILED_NUM, MetricDef.SEND_TPS,
            MetricDef.RECV_TPS, MetricDef.PROCESS_LATENCY};

    public void setMetricValue(MetricSnapshot snapshot, String metricName){
        String value = UIMetricUtils.getMetricValue(snapshot);
        setValue(metricName, value);
    }

    protected void setValue(String metricName, String value) {
        metrics.put(metricName, value);
    }

    protected String getValue(String metricName) {
        return metrics.get(metricName);
    }

    public Map<String, String> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, String> metrics) {
        this.metrics = metrics;
    }
}
