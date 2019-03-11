package com.cqx.jstorm.spout.impl;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.cqx.jstorm.spout.ISpout;
import com.cqx.jstorm.util.AppConst;

import java.util.Map;

/**
 * 分组测试
 *
 * @author chenqixu
 */
public class EmitGroupingSpout extends ISpout {

    @Override
    protected void open(Map conf, TopologyContext context) {

    }

    @Override
    protected void nextTuple() {

    }

    @Override
    protected void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("groupid", "values"));
    }
}
