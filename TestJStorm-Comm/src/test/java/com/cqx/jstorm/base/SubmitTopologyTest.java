package com.cqx.jstorm.base;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SubmitTopologyTest {

    private static Logger logger = LoggerFactory.getLogger(SubmitTopologyTest.class);

    @Test
    public void setHostAssignmentWorkers() {
        int totleWorkNum = 20;
        String ips = "10.1.8.78";
        String[] iparr = ips.split(",", -1);
        Map<String, Integer> topologyTaskParallelismMap = new HashMap<>();
        topologyTaskParallelismMap.put("spout", 1);
        topologyTaskParallelismMap.put("bolt", 20);
        logger.info("totleWorkNum % iparr.length：{}", totleWorkNum % iparr.length);
        for (int i = 0; i < totleWorkNum; i++) {
            logger.info("i % iparr.length：{}", i % iparr.length);
        }
        // spout和bolt的并发必须是worker的倍数
        for (Map.Entry<String, Integer> entry : topologyTaskParallelismMap.entrySet()) {
            if (entry.getValue() % totleWorkNum != 0) {
                logger.warn("{} parall：{} must be a multipe of worknum：{}，please check.", entry.getKey(), entry.getValue(), totleWorkNum);
                return;
            }
        }
        // 分配
        for (int i = 0; i < totleWorkNum; i++) {
            for (Map.Entry<String, Integer> entry : topologyTaskParallelismMap.entrySet()) {
                logger.info("entry.getKey()：{}, entry.getValue()：{}，entry.getValue() / totleWorkNum：{}", entry.getKey(), entry.getValue(), entry.getValue() / totleWorkNum);
            }
        }
    }

    @Test
    public void newAllocate() {
        String[] spout_array = {"s1", "s2", "s3", "s4", "s5", "s6"};
        String[] worker_array = {"w1", "w2", "w3"};
        for (int i = 0; i < spout_array.length; i++) {
            String allocate_worker = worker_array[i % worker_array.length];
//            logger.info("spout：{}，allocate worker：{}", spout_array[i], allocate_worker);
        }
        Map<String, Integer> spout = new HashMap<>();
        int totleWorkNum = 3;
        spout.put("S", 1);
        spout.put("A", 8);
        for (int i = 0; i < totleWorkNum; i++) {
            for (Map.Entry<String, Integer> entry : spout.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();
                int result = value / totleWorkNum;
                int mod = 0;
                if (result > 0) {
                    mod = value % totleWorkNum;
                    if (mod > i) {
                        logger.info("worker {}，add {}，num：{}", i, key, result + 1);
                    } else {
                        logger.info("worker {}，add {}，num：{}", i, key, result);
                    }
                } else {
                    if (value > i) {
                        logger.info("worker {}，add {}，num：{}", i, key, 1);
                    }
                }
            }
        }
    }
}