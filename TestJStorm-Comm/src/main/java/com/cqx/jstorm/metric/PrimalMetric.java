package com.cqx.jstorm.metric;

import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.metric.MetricClient;
import com.cqx.jstorm.bean.AsmHistogramBean;

import java.util.HashMap;
import java.util.Map;

/**
 * 源生监控
 *
 * @author chenqixu
 */
public class PrimalMetric implements IMetric {
    private MetricClient metricClient;
    private Map<String, AsmCounter> asmCounterMap;
    private Map<String, AsmHistogramBean> asmHistogramMap;

    @Override
    public void initMetric(TopologyContext context) {
        this.metricClient = new MetricClient(context);
        this.asmCounterMap = new HashMap<>();
        this.asmHistogramMap = new HashMap<>();
    }

    @Override
    public void registerCounter(String name) {
        asmCounterMap.put(name, metricClient.registerCounter(name));
    }

    @Override
    public void registerCounterSize(String name) {
        registerCounter(name);
    }

    @Override
    public void counterInc(String name) {
        AsmCounter asmCounter = asmCounterMap.get(name);
        if (asmCounter != null) asmCounter.inc();
    }

    @Override
    public void counterSize(String name, long value) {
        AsmCounter asmCounter = asmCounterMap.get(name);
        if (asmCounter != null) asmCounter.update(value);
    }

    @Override
    public void registerHistogram(String name) {
        asmHistogramMap.put(name, new AsmHistogramBean(metricClient.registerHistogram(name), name));
    }

    @Override
    public void histogramStart(String name) {
        AsmHistogramBean asmHistogramBean = asmHistogramMap.get(name);
        if (asmHistogramBean != null) {
            asmHistogramBean.start();
        }
    }

    @Override
    public void histogramEnd(String name) {
        AsmHistogramBean asmHistogramBean = asmHistogramMap.get(name);
        if (asmHistogramBean != null) {
            asmHistogramBean.end();
        }
    }

}
