package com.cqx.jstorm.bean;

import com.alibaba.jstorm.common.metric.AsmHistogram;

/**
 * AsmHistogramBean
 *
 * @author chenqixu
 */
public class AsmHistogramBean {
    private AsmHistogram asmHistogram;
    private long start;
    private String name;

    public AsmHistogramBean(AsmHistogram asmHistogram, String name) {
        this.asmHistogram = asmHistogram;
        this.name = name;
    }

    public void start() {
        start = asmHistogram.getTime();
    }

    public long end() {
        asmHistogram.updateTime(start);
        return 0;
    }

    public AsmHistogram getAsmHistogram() {
        return asmHistogram;
    }

    public void setAsmHistogram(AsmHistogram asmHistogram) {
        this.asmHistogram = asmHistogram;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
