package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.dpi.spout.ADBNullSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * ADBBolt
 *
 * @author chenqixu
 */
public class ADBSyncDBBolt extends IBolt {
    private static final Logger logger = LoggerFactory.getLogger(ADBSyncDBBolt.class);
    private List<Long> data = new ArrayList<>();
    private Random random = new Random();

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {

    }

    @Override
    public void execute(Tuple input) throws Exception {
        logger.info("input：{}", input);
        Long v = input.getLongByField("kafka");
        data.add(v);
        if (data.size() == 5) {
            SleepUtil.sleepSecond(random.nextInt(5));
            data.clear();
            this.collector.emit(ADBNullSpout.STREAM_REMOVE_QUEUE, new Values(5L));
        }
    }
}
