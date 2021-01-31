package com.cqx.jstorm.dpi.mbean;

/**
 * Hdfs
 *
 * @author chenqixu
 */
public class Hdfs implements HdfsMBean {

    private int all_cnt;

    @Override
    public void add(int cnt) {
        all_cnt = all_cnt + cnt;
    }

    @Override
    public int get() {
        return all_cnt;
    }
}
