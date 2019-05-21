package com.cqx.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.util.ExceptionMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 公共bolt
 *
 * @author chenqixu
 */
public class CommonBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(CommonBolt.class);
    private IBolt iBolt;
    private ExceptionMetrics exceptionMetrics;

    public CommonBolt(String bolt_name) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        this.iBolt = IBolt.generate(bolt_name);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.exceptionMetrics = ExceptionMetrics.getInstance();
        this.exceptionMetrics.registerBolt(iBolt);
        this.iBolt.setCollector(collector);
        try {
            this.iBolt.prepare(stormConf, context);
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            this.exceptionMetrics.markExceptionSingle("组件初始化发生异常", e);
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            this.iBolt.execute(input);
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            this.exceptionMetrics.markException("组件处理发生异常", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.iBolt.declareOutputFields(declarer);
    }

    @Override
    public void cleanup() {
        this.iBolt.cleanup();
    }
}
