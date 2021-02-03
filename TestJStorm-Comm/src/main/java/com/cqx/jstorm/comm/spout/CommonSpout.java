package com.cqx.jstorm.comm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IDynamicComponent;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.cqx.jstorm.comm.bean.SpoutBean;
import com.cqx.jstorm.comm.util.ExceptionMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 公共Spout
 *
 * @author chenqixu
 */
public class CommonSpout extends BaseRichSpout implements IDynamicComponent {

    private static final Logger logger = LoggerFactory.getLogger(CommonSpout.class);
    private ISpout iSpout;
    private ExceptionMetrics exceptionMetrics;

    public CommonSpout(String spout_name) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        this.iSpout = ISpout.generate(spout_name);
    }

    public CommonSpout(SpoutBean spoutBean) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        this.iSpout = ISpout.generate(spoutBean.getGenerateClassName());
        this.iSpout.setSendBeanList(spoutBean.getSendBeanList());
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.exceptionMetrics = ExceptionMetrics.getInstance(conf);
        this.exceptionMetrics.registerSpout(iSpout);
        this.iSpout.setContext(context);
        this.iSpout.setCollector(collector);
        try {
            this.iSpout.open(conf, context);
        } catch (Exception e) {
            this.exceptionMetrics.markExceptionSingle("组件初始化发生异常", e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            this.iSpout.nextTuple();
        } catch (Exception e) {
            this.exceptionMetrics.markException("组件处理发生异常", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.iSpout.declareOutputFields(declarer);
    }

    @Override
    public void ack(Object object) {
        this.iSpout.ack(object);
    }

    @Override
    public void fail(Object object) {
        this.iSpout.fail(object);
    }

    @Override
    public void close() {
        this.iSpout.close();
    }

    @Override
    public void update(Map conf) {
        this.iSpout.update((Map) conf.get("param"));
    }
}
