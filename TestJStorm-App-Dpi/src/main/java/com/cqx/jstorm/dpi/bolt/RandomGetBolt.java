package com.cqx.jstorm.dpi.bolt;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.comm.bolt.IBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 从上游获取随机数
 *
 * @author chenqixu
 */
public class RandomGetBolt extends IBolt {
    private static Logger logger = LoggerFactory.getLogger(RandomGetBolt.class);
    private String field_name;
    private String tns;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        tns = (String) stormConf.get("tns");
        field_name = getReceiveBeanList().get(0).getOutput_fields().get(0);
        logger.info("prepare，tns：{}，field_name：{}", tns, field_name);
        logger.info("[Bolt]TOPOLOGY_WORKERS：{}，getTaskToComponent：{}，getComponentIds()：{}，getThisComponentId：{}，getThisTaskId：{}，getThisTaskIndex：{}"
                , stormConf.get(Config.TOPOLOGY_WORKERS)
                , context.getTaskToComponent()
                , context.getComponentIds()
                , context.getThisComponentId()
                , context.getThisTaskId()
                , context.getThisTaskIndex());
    }

    @Override
    public void execute(Tuple input) throws Exception {
        Object value = input.getValueByField(field_name);
        logger.info("tns：{}，getValue：{}", tns, value);
    }
}
