package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.comm.spout.ISpout;

import java.util.Map;

/**
 * MsisdnTestSpout
 *
 * @author chenqixu
 */
public class MsisdnTestSpout extends ISpout {
    private int cnt = 0;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
    }

    @Override
    public void nextTuple() throws Exception {
        for (long i = 13500000000L; i < 13600000000L; i++) {
            if (cnt >= 10000) break;
            cnt++;
            long mod = i % 3;
            collector.emit(new Values(mod, i));
        }
    }
}
