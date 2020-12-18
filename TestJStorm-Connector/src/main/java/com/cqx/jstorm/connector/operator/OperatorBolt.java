package com.cqx.jstorm.connector.operator;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bolt.IBolt;

import java.util.Map;

/**
 * OperatorBolt
 *
 * @author chenqixu
 */
public class OperatorBolt extends IBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {

    }

    @Override
    public void execute(Tuple input) throws Exception {

    }
}
