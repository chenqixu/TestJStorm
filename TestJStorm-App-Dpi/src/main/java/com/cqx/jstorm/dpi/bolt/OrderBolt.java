package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.codahale.metrics.Meter;
import com.cqx.jstorm.comm.bean.ReceiveBean;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.comm.metric.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 顺序测试Bolt
 *
 * @author chenqixu
 */
public class OrderBolt extends IBolt {
    private static final Logger logger = LoggerFactory.getLogger(OrderBolt.class);
    private int rec_cnt = 0;
    private Meter bl;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        bl = MetricUtils.getMeter("meter.bl.count");
    }

    @Override
    public void execute(Tuple input) throws Exception {
        String rec = input.getStringByField("order");
//        logger.info("rec：{}", rec);
        rec_cnt++;
        bl.mark();
        this.collector.ack(input);
    }

    @Override
    public void cleanup() {
        logger.info("order_close，rec_cnt：{}", rec_cnt);
    }
}
