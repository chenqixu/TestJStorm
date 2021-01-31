package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.comm.util.AppConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * GetBolt
 *
 * @author chenqixu
 */
public class GetBolt extends IBolt {
    private static Logger logger = LoggerFactory.getLogger(GetBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
    }

    @Override
    public void execute(Tuple input) throws Exception {
        int value = (Integer) input.getValueByField(AppConst.FIELDS);
        //判断是单还是双
        if (value % 2 == 0) {
            logger.info("fail.value：{}", value);
            this.collector.fail(input);
        } else {
            logger.info("value：{}", value);
        }
    }
}
