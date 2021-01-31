package com.cqx.jstorm.comm.bean;

import java.util.Map;

/**
 * 拓扑
 *
 * @author chenqixu
 */
public class TopologyBean {
    private int worker_num;
    private int ack_num;
    private long worker_memory;
    private int cpu_slotNum;
    private String jvm_options;
    private String name;
    private String ip;

    public static TopologyBean newbuilder() {
        return new TopologyBean();
    }

    public TopologyBean parser(Object param) {
        Map<String, ?> tmp = (Map<String, ?>) param;
        worker_num = (Integer) tmp.get("worker_num");
        ack_num = (Integer) tmp.get("ack_num");
        worker_memory = Long.valueOf(tmp.get("worker_memory").toString());
        cpu_slotNum = (Integer) tmp.get("cpu_slotNum");
        jvm_options = (String) tmp.get("jvm_options");
        name = (String) tmp.get("name");
        ip = (String) tmp.get("ip");
        return this;
    }

    public int getWorker_num() {
        return worker_num;
    }

    public void setWorker_num(int worker_num) {
        this.worker_num = worker_num;
    }

    public int getAck_num() {
        return ack_num;
    }

    public void setAck_num(int ack_num) {
        this.ack_num = ack_num;
    }

    public long getWorker_memory() {
        return worker_memory;
    }

    public void setWorker_memory(long worker_memory) {
        this.worker_memory = worker_memory;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getCpu_slotNum() {
        return cpu_slotNum;
    }

    public void setCpu_slotNum(int cpu_slotNum) {
        this.cpu_slotNum = cpu_slotNum;
    }

    public String getJvm_options() {
        return jvm_options;
    }

    public void setJvm_options(String jvm_options) {
        this.jvm_options = jvm_options;
    }
}
