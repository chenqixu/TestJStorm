package com.cqx.jstorm.bolt.impl;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.spout.impl.EmitDpiSpout;

import java.util.Map;

/**
 * EmitDpiErrBolt
 *
 * @author chenqixu
 */
public class EmitDpiErrBolt extends IBolt {

    @Override
    protected void prepare(Map stormConf, TopologyContext context) throws Exception {
        logger.info("getThisComponentId：{}，getThisTaskId：{}，getThisTaskIndex：{}",
                context.getThisComponentId(), context.getThisTaskId(), context.getThisTaskIndex());
    }

    @Override
    protected void execute(Tuple input) throws Exception {
        String values = input.getStringByField(EmitDpiSpout.VALUES);
        logger.info("receive error data：{}，prepare to save local file. valueList：{}", values, input.getValues());
    }
}
