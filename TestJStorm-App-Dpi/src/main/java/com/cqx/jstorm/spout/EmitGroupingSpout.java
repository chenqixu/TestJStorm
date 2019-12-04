package com.cqx.jstorm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.Utils;

import java.util.Map;

/**
 * 分组测试
 *
 * @author chenqixu
 */
public class EmitGroupingSpout extends ISpout {

    private static final String GET_AND_SEND1 = "GET_AND_SEND1";
    private static final String GET_AND_SEND2 = "GET_AND_SEND2";
    private long cnt = 0L;

    @Override
    public void open(Map conf, TopologyContext context) {
    }

    @Override
    public void nextTuple() {
        String value = "EmitGroupingSpout_" + getCnt() + "_";
        this.collector.emit(GET_AND_SEND1, new Values(value + GET_AND_SEND1));
        this.collector.emit(GET_AND_SEND2, new Values(value + GET_AND_SEND2));
        Utils.sleep(3000);
    }

    @Override
    protected void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(GET_AND_SEND1, new Fields(AppConst.FIELDS));
        declarer.declareStream(GET_AND_SEND2, new Fields(AppConst.FIELDS));
    }

    private Long getCnt() {
        cnt++;
        if (cnt > 100000000L) cnt = 0L;
        return cnt;
    }
}
