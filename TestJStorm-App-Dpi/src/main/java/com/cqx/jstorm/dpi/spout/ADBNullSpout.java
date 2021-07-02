package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.comm.spout.ISpout;
import com.cqx.jstorm.comm.util.kafka.KafkaConsumerGRUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ADBNullSpout
 *
 * @author chenqixu
 */
public class ADBNullSpout extends ISpout {
    public static final String STREAM_REMOVE_QUEUE = "STREAM_REMOVE_QUEUE";
    public static final String STREAM_CONSUMER = "STREAM_CONSUMER";
    private static final Logger logger = LoggerFactory.getLogger(ADBNullSpout.class);
    private static final long NO_CURRENT_THREAD = -1L;
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private volatile long sleepTime = 1000L;
    private long cnt = 0L;
    private KafkaConsumerGRUtil kafkaConsumerGRUtil;
    private AtomicBoolean isClose = new AtomicBoolean(false);

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        logger.info("TaskToComponent：{}", context.getTaskToComponent());
        logger.info("ComponentTask：{}，size：{}", context.getComponentTasks("ADBConsumerBolt"),
                context.getComponentTasks("ADBConsumerBolt").size());
        sleepTime = ParamUtil.setValDefault(conf, "sleepTime", 1000L);
        logger.info("sleepTime：{}", sleepTime);
        kafkaConsumerGRUtil = new KafkaConsumerGRUtil(conf);
        kafkaConsumerGRUtil.subscribe((String) conf.get("topic"));
    }

    @Override
    public void nextTuple() throws Exception {
        if (!isClose.get()) kafkaConsumerGRUtil.poll(1000L);
        // 间隔x毫秒下发一个空包
        SleepUtil.sleepMilliSecond(sleepTime);
        this.collector.emit(new Values(cnt++));
        logger.info("间隔{}毫秒下发一个空包，cnt：{}，isClose：{}", sleepTime, cnt, isClose.get());
    }

    @Override
    public void update(Map conf) {
        try {
            sleepTime = ParamUtil.setValDefault(conf, "sleepTime", 1000L);

            long threadId = Thread.currentThread().getId();
//            logger.info("threadId：{}，currentThread.get()：{}，equal：{}"
//                    , threadId, currentThread.get(), threadId != currentThread.get());
            boolean not_compareAndSet = !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId);
//            logger.info("!compareAndSet：{}", !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId));

            if (kafkaConsumerGRUtil != null) {
                kafkaConsumerGRUtil.close();
                isClose.set(true);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.error("动态更新配置异常：" + e.getMessage(), e);
        }
        logger.info("update sleepTime：{}", sleepTime);
    }

    @Override
    public void close() {
        logger.info("关闭应用，cnt：{}", cnt);
        if (kafkaConsumerGRUtil != null) kafkaConsumerGRUtil.close();
    }
}
