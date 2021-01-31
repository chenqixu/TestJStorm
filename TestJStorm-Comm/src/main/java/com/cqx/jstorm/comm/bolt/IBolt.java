package com.cqx.jstorm.comm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.comm.bean.ReceiveBean;
import com.cqx.jstorm.comm.bean.SendBean;
import com.cqx.jstorm.comm.metric.CommonMetric;
import com.cqx.jstorm.comm.util.AppConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
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
    private List<ReceiveBean> receiveBeanList;
    private List<SendBean> sendBeanList;

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

    public void cleanup() {
    }

    public List<ReceiveBean> getReceiveBeanList() {
        return receiveBeanList;
    }

    public void setReceiveBeanList(List<ReceiveBean> receiveBeanList) {
        this.receiveBeanList = receiveBeanList;
    }

    public List<SendBean> getSendBeanList() {
        return sendBeanList;
    }

    public void setSendBeanList(List<SendBean> sendBeanList) {
        this.sendBeanList = sendBeanList;
    }
}
