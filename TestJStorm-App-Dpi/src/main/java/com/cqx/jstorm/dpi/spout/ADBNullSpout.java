package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.comm.spout.ISpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * ADBNullSpout
 *
 * @author chenqixu
 */
public class ADBNullSpout extends ISpout {
    public static final String STREAM_REMOVE_QUEUE = "STREAM_REMOVE_QUEUE";
    public static final String STREAM_CONSUMER = "STREAM_CONSUMER";
    private static final Logger logger = LoggerFactory.getLogger(ADBNullSpout.class);

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        logger.info("TaskToComponent：{}", context.getTaskToComponent());
        logger.info("ComponentTask：{}，size：{}", context.getComponentTasks("ADBConsumerBolt"),
                context.getComponentTasks("ADBConsumerBolt").size());
    }

    @Override
    public void nextTuple() throws Exception {
        // 间隔500毫秒下发一个空包
        SleepUtil.sleepMilliSecond(500L);
        this.collector.emit(STREAM_CONSUMER, new Values("0"));
        logger.info("间隔500毫秒下发一个空包");
    }
}
