package com.cqx.jstorm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * 随机下发数字
 *
 * @author chenqixu
 */
public class RandomSpout extends ISpout {
    private static Logger logger = LoggerFactory.getLogger(RandomSpout.class);
    private Random random;
    private int sed;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        random = new Random();
        sed = ((Number) conf.get("s-random")).intValue();
        logger.info("open，s-random：{}", sed);
    }

    @Override
    public void nextTuple() throws Exception {
        int randomInt = random.nextInt(sed);
        this.collector.emit(new Values(randomInt));
        logger.info("send：{}", randomInt);
        Utils.sleep(randomInt);
    }
}
