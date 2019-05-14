package com.cqx.jstorm.bolt.impl;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bolt.IBolt;

import java.util.Map;

/**
 * EmitDpiKafkaBolt
 *
 * @author chenqixu
 */
public class EmitDpiKafkaBolt extends IBolt {

    @Override
    protected void prepare(Map stormConf, TopologyContext context) throws Exception {
        logger.info("getThisComponentId：{}，getThisTaskId：{}，getThisTaskIndex：{}",
                context.getThisComponentId(), context.getThisTaskId(), context.getThisTaskIndex());
    }

    @Override
    protected void execute(Tuple input) throws Exception {
        Object values = input.getValueByField(EmitDpiIBolt.KAFKA_FIELDS);
        logger.info("receive kafka data：{}，prepare to push to kafka. valueList：{}", values, input.getValues());
    }
}
