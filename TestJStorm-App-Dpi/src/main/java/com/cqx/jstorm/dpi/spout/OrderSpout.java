package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.codahale.metrics.Meter;
import com.cqx.jstorm.comm.metric.MetricUtils;
import com.cqx.jstorm.comm.spout.ISpout;
import com.cqx.jstorm.comm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 顺序测试Spout
 *
 * @author chenqixu
 */
public class OrderSpout extends ISpout {
    private static final Logger logger = LoggerFactory.getLogger(OrderSpout.class);
    private int send_cnt = 0;
    private Random random;
    private List<Integer> sends;
    private int all_cnt = 0;
    private int fail_cnt = 0;
    private int real_send_cnt = 0;
    private Meter sp;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        random = new Random();
        sends = new ArrayList<>();

        MetricUtils.reset();
        MetricUtils.build(5, TimeUnit.SECONDS);
        sp = MetricUtils.getMeter("meter.sp.count");
    }

    @Override
    public void nextTuple() throws Exception {
        sends.clear();
        for (int i = 0; i < 5; i++) {
            int r = random.nextInt(1000);
            sends.add(r);
            collector.emit(new Values(send_cnt + "_" + r), grenerateUUIDMessageId());
            real_send_cnt++;
            sp.mark();
            Utils.sleep(1);
        }
        send_cnt++;
    }

    @Override
    public void ack(Object object) {
        all_cnt++;
    }

    @Override
    public void fail(Object object) {
        fail_cnt++;
    }

    @Override
    public void close() {
        logger.info("order_close，all_cnt：{}，fail_cnt：{}，real_send_cnt：{}",
                all_cnt, fail_cnt, real_send_cnt);
    }
}
