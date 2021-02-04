package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.comm.spout.ISpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SendSpout
 *
 * @author chenqixu
 */
public class SendSpout extends ISpout {
    private static Logger logger = LoggerFactory.getLogger(SendSpout.class);
    private Random random;
    private long firstSendTime;
    private long sendCnt = 0L;
    private int maxSend = 12000;
    private AtomicBoolean first = new AtomicBoolean(true);

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        random = new Random();
        maxSend = ((Number) conf.get("maxSend")).intValue();
        logger.info("open，maxSend：{}", maxSend);
    }

    @Override
    public void nextTuple() throws Exception {
        if (first.getAndSet(false)) {
            firstSendTime = System.currentTimeMillis();
            logger.info("firstSendTime：{}", firstSendTime);
        }
        long interval = System.currentTimeMillis() - firstSendTime;
        //当前时间离首次发送的间隔，小于1秒
        if (interval < 1000) {
            //本次发送达到maxSend，就停止发送
            if (sendCnt < maxSend) {
                int randomInt = random.nextInt(1000);
                collector.emit(new Values(randomInt));
                sendCnt++;
            }
        }
        //重置首次发送的时间，重置sendCnt为0
        else {
            logger.info("stop send，sendCnt：{}，firstSendTime：{}", sendCnt, firstSendTime);
            firstSendTime = System.currentTimeMillis();
            sendCnt = 0;
        }
    }

    @Override
    public void update(Map conf) {
        maxSend = ((Number) conf.get("maxSend")).intValue();
        logger.info("触发更新：{}", maxSend);
    }
}
