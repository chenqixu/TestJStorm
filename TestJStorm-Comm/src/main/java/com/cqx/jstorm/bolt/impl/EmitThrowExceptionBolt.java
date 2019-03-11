package com.cqx.jstorm.bolt.impl;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.util.AppConst;

import java.util.Map;

/**
 * EmitThrowExceptionBolt
 *
 * @author chenqixu
 */
public class EmitThrowExceptionBolt extends IBolt {

    @Override
    protected void prepare(Map stormConf, TopologyContext context) throws Exception {
        logger.info("####prepare");
        Runtime.getRuntime().addShutdownHook(new Thread("release-shutdown-hook-EmitThrowExceptionBolt：" + this) {
            @Override
            public void run() {
                logger.info("☆☆☆ release-shutdown-hook-准备强制释放资源。");
            }
        });
        throw new Exception("bolttest init throw Exception.");
    }

    @Override
    protected void execute(Tuple input) throws Exception {
        logger.info("####{} to execute，input：{}", this, input.getStringByField(AppConst.FIELDS));
//        throw new Exception("bolttest throw Exception.");
    }

    @Override
    protected void cleanup() {
        logger.info("####{} to_cleanup", this);
    }
}
