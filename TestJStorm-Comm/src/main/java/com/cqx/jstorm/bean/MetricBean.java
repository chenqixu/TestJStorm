package com.cqx.jstorm.bean;

import com.cqx.jstorm.metric.MetricType;

/**
 * MetricBean
 *
 * @author chenqixu
 */
public class MetricBean {
    private String name;
    private MetricType metricType;
    private volatile long filenum = 0;
    private Object lock = new Object();

    public MetricBean(String name, MetricType metricType) {
        this.name = name;
        this.metricType = metricType;
    }

    public void Increment(long filenum) {
        synchronized (lock) {
            this.filenum += filenum;
        }
    }

    public void clean() {
        synchronized (lock) {
            this.filenum = 0;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getFilenum() {
        return filenum;
    }

    public void setFilenum(long filenum) {
        this.filenum = filenum;
    }

    public MetricType getMetricType() {
        return metricType;
    }
}
