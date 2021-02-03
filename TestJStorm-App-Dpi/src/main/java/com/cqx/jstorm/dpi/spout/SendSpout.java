package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.comm.spout.ISpout;
import com.cqx.jstorm.comm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * SendSpout
 *
 * @author chenqixu
 */
public class SendSpout extends ISpout {
    private static Logger logger = LoggerFactory.getLogger(SendSpout.class);
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

    @Override
    public void update(Map conf) {
        sed = ((Number) conf.get("s-random")).intValue();
        logger.info("触发更新：{}", sed);
    }
}
