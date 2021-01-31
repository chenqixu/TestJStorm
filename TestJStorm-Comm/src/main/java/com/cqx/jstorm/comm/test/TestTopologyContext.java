package com.cqx.jstorm.comm.test;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * 用于测试
 *
 * @author chenqixu
 */
public class TestTopologyContext extends TopologyContext {

    public TestTopologyContext(Map conf) {
        super(null, conf, null, null, null, null,
                null, null, null, null, null, null, null,
                null, null, null, null);
    }

    public static TestTopologyContext builder(Map conf) {
        conf.put(Config.STORM_CLUSTER_MODE, "local");
        return new TestTopologyContext(conf);
    }

    public int getThisTaskIndex() {
        return 0;
    }

    public String getThisComponentId() {
        return "testbolt";
    }

    public int getThisTaskId() {
        return 0;
    }

}
