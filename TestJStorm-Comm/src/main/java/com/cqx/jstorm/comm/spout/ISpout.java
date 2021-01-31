package com.cqx.jstorm.comm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.cqx.jstorm.comm.bean.SendBean;
import com.cqx.jstorm.comm.metric.CommonMetric;
import com.cqx.jstorm.comm.util.AppConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    private List<SendBean> sendBeanList;

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

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (getSendBeanList() != null) {
            for (SendBean sendBean : getSendBeanList()) {
                if (sendBean.getStreamId() != null) {
                    declarer.declareStream(sendBean.getStreamId(), new Fields(sendBean.getOutput_fields()));
                } else {
                    declarer.declare(new Fields(sendBean.getOutput_fields()));
                }
            }
        } else {
            declarer.declare(new Fields(AppConst.FIELDS));
        }
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

    public Object grenerateUUIDMessageId() {
        return UUID.randomUUID().toString();
    }

    public List<SendBean> getSendBeanList() {
        return sendBeanList;
    }

    public void setSendBeanList(List<SendBean> sendBeanList) {
        this.sendBeanList = sendBeanList;
    }
}
