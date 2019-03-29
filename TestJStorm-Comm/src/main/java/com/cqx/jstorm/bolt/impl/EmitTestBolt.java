package com.cqx.jstorm.bolt.impl;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 提交测试
 *
 * @author chenqixu
 */
public class EmitTestBolt extends IBolt {

    private AtomicInteger count = new AtomicInteger(0);

    @Override
    protected void prepare(Map stormConf, TopologyContext context) {
        logger.info("####prepare");
//        Utils.sleep(60000);
//        logger.info("####1 bolt prepare sleep 60000");
//        Utils.sleep(60000);
//        logger.info("####2 bolt prepare sleep 60000");
//        Utils.sleep(55000);
//        logger.info("####3 bolt prepare sleep 55000");
//        Utils.sleep(55000);
//        logger.info("####4 bolt prepare sleep 55000");
//        Utils.sleep(2000);
//        logger.info("####5 bolt prepare sleep 2000");
//        Utils.sleep(2000);
//        logger.info("####6 bolt prepare sleep 2000");
//        Utils.sleep(2000);
//        logger.info("####7 bolt prepare sleep 2000");
//        Utils.sleep(2000);
//        logger.info("####8 bolt prepare sleep 2000");
        Utils.sleep(2000);
        logger.info("####9 bolt prepare sleep 2000");
    }

    @Override
    protected void execute(Tuple input) {
        logger.info("####{} to execute，input：{}，count：{}", this, input.getStringByField(AppConst.FIELDS), count.incrementAndGet());
    }

    protected void cleanup() {
        logger.info("####{} to cleanup，count：{}",
                this, count.get());
    }
}
