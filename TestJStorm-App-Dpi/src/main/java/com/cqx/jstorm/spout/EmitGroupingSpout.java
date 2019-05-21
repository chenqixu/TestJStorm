package com.cqx.jstorm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.cqx.jstorm.spout.ISpout;

import java.util.Map;

/**
 * 分组测试
 *
 * @author chenqixu
 */
public class EmitGroupingSpout extends ISpout {

    @Override
    public void open(Map conf, TopologyContext context) {

    }

    @Override
    public void nextTuple() {

    }

    @Override
    protected void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("groupid", "values"));
    }
}
