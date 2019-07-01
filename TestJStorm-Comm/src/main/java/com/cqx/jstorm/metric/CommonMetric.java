package com.cqx.jstorm.metric;

import backtype.storm.task.TopologyContext;

/**
 * IMetric
 *
 * @author chenqixu
 */
public class CommonMetric {
    private IMetric iMetric;
    private boolean isTest = false;

    public void initMetric(TopologyContext context) {
        if (isTest) {
            iMetric = new LocalMetric();
        } else {
            iMetric = new PrimalMetric();
        }
        iMetric.initMetric(context);
    }

    public void registerCounter(String name) {
        iMetric.registerCounter(name);
    }

    public void registerCounterSize(String name) {
        iMetric.registerCounterSize(name);
    }

    public void counterInc(String name) {
        iMetric.counterInc(name);
    }

    public void counterSize(String name, long value) {
        iMetric.counterSize(name, value);
    }

    public void registerHistogram(String name) {
        iMetric.registerHistogram(name);
    }

    public void histogramStart(String name) {
        iMetric.histogramStart(name);
    }

    public void histogramEnd(String name) {
        iMetric.histogramEnd(name);
    }

    public boolean isTest() {
        return isTest;
    }

    public void setTest(boolean test) {
        isTest = test;
    }
}
