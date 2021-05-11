package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.codahale.metrics.Meter;
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
        // 1worker 3并发情况下
        //OrderBolt:4-BoltExecutors] TaskId：4，TaskIndex：2，ComponentId：OrderBolt
        //OrderBolt:3-BoltExecutors] TaskId：3，TaskIndex：1，ComponentId：OrderBolt
        //OrderBolt:2-BoltExecutors] TaskId：2，TaskIndex：0，ComponentId：OrderBolt
        logger.info("TaskId：{}，TaskIndex：{}，ComponentId：{}",
                context.getThisTaskId(), context.getThisTaskIndex(), context.getThisComponentId());
        // 2worker 6并发情况下
        //16801
        //OrderBolt:2-BoltExecutors] TaskId：2，TaskIndex：0，ComponentId：OrderBolt
        //OrderBolt:4-BoltExecutors] TaskId：4，TaskIndex：2，ComponentId：OrderBolt
        //OrderBolt:3-BoltExecutors] TaskId：3，TaskIndex：1，ComponentId：OrderBolt
        //16802
        //OrderBolt:5-BoltExecutors] TaskId：5，TaskIndex：3，ComponentId：OrderBolt
        //OrderBolt:6-BoltExecutors] TaskId：6，TaskIndex：4，ComponentId：OrderBolt
        //OrderBolt:7-BoltExecutors] TaskId：7，TaskIndex：5，ComponentId：OrderBolt
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
