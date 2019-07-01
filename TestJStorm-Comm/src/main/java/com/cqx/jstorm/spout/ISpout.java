package com.cqx.jstorm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.cqx.jstorm.metric.CommonMetric;
import com.cqx.jstorm.util.AppConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 公共Spout接口
 *
 * @author chenqixu
 */
public abstract class ISpout extends CommonMetric implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ISpout.class);
    protected SpoutOutputCollector collector;
    private TopologyContext context;
    private AtomicInteger batchIndex;
    private String taskInfo;

    public static ISpout generate(String name) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class cls = Class.forName(name);
        return (ISpout) cls.newInstance();
    }

    public void setContext(TopologyContext context) {
        this.context = context;
        this.batchIndex = new AtomicInteger();
        this.taskInfo = this.context.getThisComponentId() + this.context.getThisTaskId() + this.context.getThisTaskIndex() + "@";
        initMetric(this.context);
    }

    public void setCollector(SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public abstract void open(Map conf, TopologyContext context) throws Exception;

    public abstract void nextTuple() throws Exception;

    protected void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AppConst.FIELDS));
    }

    public void ack(Object object) {
    }

    public void fail(Object object) {
    }

    public void close() {
    }

    public String grenerateMessageId() {
        if (batchIndex == null) throw new NullPointerException("context is null, need init.");
        int result = batchIndex.getAndIncrement();
        if (batchIndex.get() > 10000000) batchIndex.set(0);
        return taskInfo + result;
    }
}
