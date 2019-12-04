package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.util.AppConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * EmitGetAndSend1Bolt
 *
 * @author chenqixu
 */
public class EmitGetAndSend1Bolt extends IBolt {

    private static final Logger logger = LoggerFactory.getLogger(EmitGetAndSend1Bolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {

    }

    @Override
    public void execute(Tuple input) throws Exception {
        String value = input.getStringByField(AppConst.FIELDS);
        logger.info("####{} to execute，input：{}", this, value);
        this.collector.emit(new Values("GetAndSend1_" + value));
    }
}
