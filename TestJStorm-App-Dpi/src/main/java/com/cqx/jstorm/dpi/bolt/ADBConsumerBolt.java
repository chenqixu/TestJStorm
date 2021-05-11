package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.dpi.spout.ADBNullSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ADBConsumerBolt
 *
 * @author chenqixu
 */
public class ADBConsumerBolt extends IBolt {
    private static final Logger logger = LoggerFactory.getLogger(ADBConsumerBolt.class);
    private AtomicLong sendQueue = new AtomicLong(0L);
    private int queue_limit = 10;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {

    }

    @Override
    public void execute(Tuple input) throws Exception {
        String streamId = input.getSourceStreamId();
        logger.info("收到：{}，数据：{}", streamId, input);
        if (streamId.equals(ADBNullSpout.STREAM_CONSUMER)) {
            // 消费kafka
            if (sendQueue.get() >= queue_limit) {
                logger.info("队列满了，等待500毫秒");
                SleepUtil.sleepMilliSecond(500L);
                return;
            }
            Values values = new Values();
            values.add(sendQueue.get());
            this.collector.emit(values);
            sendQueue.incrementAndGet();
            logger.info("消费kafka，下发记录给SyncDB进行处理：{}", values);
        } else if (streamId.equals(ADBNullSpout.STREAM_REMOVE_QUEUE)) {
            // 处理完成
            Long remove = input.getLongByField("remove");
            logger.info("处理完成，队列移除：{}", remove);
            for (int i = 0; i < remove; i++) sendQueue.decrementAndGet();
        }
    }
}
