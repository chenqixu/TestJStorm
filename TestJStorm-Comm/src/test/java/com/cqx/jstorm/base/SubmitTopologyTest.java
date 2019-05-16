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
        int totleWorkNum = 6;
        String ips = "10.1.8.78,10.1.8.81,10.1.8.75";
        String[] iparr = ips.split(",", -1);
        Map<String, Integer> topologyTaskParallelismMap = new HashMap<>();
        topologyTaskParallelismMap.put("spout", 6);
        topologyTaskParallelismMap.put("bolt", 12);
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
}