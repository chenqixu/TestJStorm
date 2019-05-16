package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.utils.DpiSocketClient;

import java.util.Map;

/**
 * EmitSocketClientBolt
 *
 * @author chenqixu
 */
public class EmitSocketClientBolt extends IBolt {

    private DpiSocketClient dpiSocketClient;

    @Override
    protected void prepare(Map stormConf, TopologyContext context) throws Exception {
        dpiSocketClient = new DpiSocketClient("127.0.0.1", 10991);
        dpiSocketClient.connect();
    }

    @Override
    protected void execute(Tuple input) throws Exception {
        String value = input.getStringByField(AppConst.FIELDS);
        dpiSocketClient.sendMsg(value);
    }

    @Override
    protected void cleanup() {
        dpiSocketClient.disconnect();
    }
}
