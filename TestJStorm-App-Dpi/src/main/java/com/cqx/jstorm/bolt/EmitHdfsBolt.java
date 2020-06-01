package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * EmitHdfsBolt
 *
 * @author chenqixu
 */
public class EmitHdfsBolt extends IBolt {

    private static final Logger logger = LoggerFactory.getLogger(EmitHdfsBolt.class);
    private TimeCostUtil timeCostUtil;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        timeCostUtil = new TimeCostUtil(true);
    }

    @Override
    public void execute(Tuple input) throws Exception {
        timeCostUtil.start();
        List<String> objectList = (List<String>) input.getValueByField(AppConst.FIELDS);
        timeCostUtil.stopAndIncrementCost();
        logger.info("objectList.size：{}，IncrementCost：{}", objectList.size(), timeCostUtil.getIncrementCost());
    }
}
