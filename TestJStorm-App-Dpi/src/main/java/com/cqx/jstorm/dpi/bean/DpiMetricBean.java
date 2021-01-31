package com.cqx.jstorm.dpi.bean;

/**
 * DpiMetricBean
 *
 * @author chenqixu
 */
public class DpiMetricBean {
    private String name;
    private volatile long filenum = 0;
    private Object lock = new Object();

    public DpiMetricBean(String name) {
        this.name = name;
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
}
