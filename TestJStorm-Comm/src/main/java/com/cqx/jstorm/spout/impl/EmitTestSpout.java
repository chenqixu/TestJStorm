package com.cqx.jstorm.spout.impl;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.spout.ISpout;
import com.cqx.jstorm.util.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 提交测试
 *
 * @author chenqixu
 */
public class EmitTestSpout extends ISpout {

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Override
    protected void open(Map conf, TopologyContext context) {
        logger.info("####open");
    }

    @Override
    protected void nextTuple() throws Exception {
        int emit_cnt = atomicInteger.getAndIncrement();
        this.collector.emit(new Values(this.toString() + "####" + emit_cnt));
        logger.info("####emit：{}", emit_cnt);
        logger.info("####sleep 500");
        Utils.sleep(500);
//        throw new Exception("spouttest throw Exception.");
    }

    @Override
    protected void close() {
        logger.info("####{} to_cleanup", this);
    }
}
