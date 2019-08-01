package com.cqx.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.metric.CommonMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * 公共Bolt接口
 *
 * @author chenqixu
 */
public abstract class IBolt extends CommonMetric implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(IBolt.class);
    protected OutputCollector collector;
    private TopologyContext context;

    public static IBolt generate(String name) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class cls = Class.forName(name);
        return (IBolt) cls.newInstance();
    }

    public void setContext(TopologyContext context) {
        this.context = context;
        initMetric(this.context);
    }

    public void setCollector(OutputCollector collector) {
        this.collector = collector;
    }

    public abstract void prepare(Map stormConf, TopologyContext context) throws Exception;

    public abstract void execute(Tuple input) throws Exception;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}
