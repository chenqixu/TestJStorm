package com.cqx.jstorm.dpi.bean;

import com.cqx.common.annotation.BeanDesc;
import com.cqx.common.bean.javabean.BaseBean;

/**
 * FastFailureBean
 *
 * @author chenqixu
 */
public class FastFailureBean extends BaseBean {
    @BeanDesc(value = "下游并发个数")
    private int bolt_num;
    @BeanDesc(value = "SPOUT执行间隔")
    private long spout_next_run;
    @BeanDesc(value = "BOLT随机休眠的基础值")
    private int bolt_sleep;

    public int getBolt_num() {
        return bolt_num;
    }

    public void setBolt_num(int bolt_num) {
        this.bolt_num = bolt_num;
    }

    public long getSpout_next_run() {
        return spout_next_run;
    }

    public void setSpout_next_run(long spout_next_run) {
        this.spout_next_run = spout_next_run;
    }

    public int getBolt_sleep() {
        return bolt_sleep;
    }

    public void setBolt_sleep(int bolt_sleep) {
        this.bolt_sleep = bolt_sleep;
    }
}
