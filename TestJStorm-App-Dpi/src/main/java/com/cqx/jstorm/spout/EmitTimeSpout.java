package com.cqx.jstorm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * EmitTimeSpout
 *
 * @author chenqixu
 */
public class EmitTimeSpout extends ISpout {

    private static final Logger logger = LoggerFactory.getLogger(EmitTimeSpout.class);
    private List<String> sum_times = new ArrayList<>();
    private int x = 0;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        for (int j = 9; j < 24; j++) {
            String _j = j + "";
            for (int i = 0; i < 60; i++) {
                String _i = i + "";
                sum_times.add("2019-08-01 " + (_j.length() == 1 ? "0" + _j : _j) + ":"
                        + (_i.length() == 1 ? "0" + _i : _i) + ":00");
            }
        }
        for (int i = 0; i < 60; i++) {
            String _i = i + "";
            sum_times.add("2019-08-02 00:" + (_i.length() == 1 ? "0" + _i : _i) + ":00");
        }
    }

    @Override
    public void nextTuple() throws Exception {
        if (x < sum_times.size()) {
            this.collector.emit(new Values(sum_times.get(x)));
            x++;
        }
    }
}
