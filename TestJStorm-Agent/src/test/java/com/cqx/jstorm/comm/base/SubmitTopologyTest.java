package com.cqx.jstorm.comm.base;

import backtype.storm.topology.TopologyBuilder;
import com.cqx.common.test.TestBase;
import com.cqx.common.utils.system.ReflectionUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SubmitTopologyTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(SubmitTopologyTest.class);

    @Test
    public void setHostAssignmentWorkers() {
        int totleWorkNum = 70;
        String ips = "10.48.134.118,10.48.134.120,10.48.134.121,10.48.134.122,10.48.134.123,10.48.134.124,10.48.134.125,10.48.134.126,10.48.134.127,10.48.134.128,10.48.134.129,10.48.134.130,10.48.134.131,10.48.134.132,10.48.134.133,10.48.134.134,10.48.134.135";
        String[] iparr = ips.split(",", -1);
        Map<String, Integer> topologyTaskParallelismMap = new HashMap<>();
        topologyTaskParallelismMap.put("spout", 140);
        topologyTaskParallelismMap.put("bolt", 140);
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

    @Test
    public void setHostAssignmentWorkersTest() throws Exception {
        Map param = getParam("realtime_location_altibase62.config.yaml");
        SubmitTopology st = SubmitTopology.builder();
        st.setAppConst(param);
        // 创建topology的生成器
        TopologyBuilder builder = new TopologyBuilder();
        // 创建Spout，因为是私有方法，需要用到反射
        ReflectionUtil.invokeMethod(st, "addSpout"
                , new Class[]{TopologyBuilder.class}, new Object[]{builder});
        // 创建bolt，因为是私有方法，需要用到反射
        ReflectionUtil.invokeMethod(st, "addBolt"
                , new Class[]{TopologyBuilder.class}, new Object[]{builder});
        // 分配策略，因为是私有方法，需要用到反射
        ReflectionUtil.invokeMethod(st, "setHostAssignmentWorkers"
                , new Class[]{Map.class}, new Object[]{param});
    }
}