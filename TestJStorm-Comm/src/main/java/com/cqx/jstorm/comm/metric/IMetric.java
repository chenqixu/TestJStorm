package com.cqx.jstorm.comm.metric;

import backtype.storm.task.TopologyContext;

/**
 * IMetric
 *
 * @author chenqixu
 */
public interface IMetric {

    void initMetric(TopologyContext context);

    void registerCounter(String name);

    void registerCounterSize(String name);

    void counterInc(String name);

    void counterSize(String name, long value);

    void registerHistogram(String name);

    void histogramStart(String name);

    void histogramEnd(String name);

}
