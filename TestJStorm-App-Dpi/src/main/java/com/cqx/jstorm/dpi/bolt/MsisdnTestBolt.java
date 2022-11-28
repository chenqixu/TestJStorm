package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.comm.bolt.IBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MsisdnTestBolt
 *
 * @author chenqixu
 */
public class MsisdnTestBolt extends IBolt {
    private static Logger logger = LoggerFactory.getLogger(MsisdnTestBolt.class);
    private String field1_name;
    private String field2_name;
    private int count;
    private int ComponentTaskSize;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        field1_name = getReceiveBeanList().get(0).getOutput_fields().get(0);
        field2_name = getReceiveBeanList().get(0).getOutput_fields().get(1);
        ComponentTaskSize = context.getComponentTasks("MsisdnTestBolt").size();
        logger.info("MsisdnTestBolt.ComponentTaskSize: {}, TaskId: {}, TaskIndex: {}"
                , ComponentTaskSize, context.getThisTaskId(), context.getThisTaskIndex());
    }

    @Override
    public void execute(Tuple input) throws Exception {
        count++;
        String mod = input.getValueByField(field1_name).toString();
        String value = input.getValueByField(field2_name).toString();
        logger.info("mod: {}, getValue: {}, mod.hashCode: {}, getMod: {}, getListMod: {}, count: {}"
                , mod, value, mod.hashCode(), getMod(mod), getListMod(mod), count);
    }

    private int getMod(String value) {
        int hashcode = value.hashCode();
        return Math.abs(hashcode % ComponentTaskSize);
    }

    private int getListMod(String value) {
        List<Object> ret = new ArrayList<Object>(1);
        ret.add(value);
        int hashcode = ret.hashCode();
        return Math.abs(hashcode % ComponentTaskSize);
    }
}
