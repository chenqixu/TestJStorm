package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.jstorm.dpi.bean.FastFailureBean;
import com.cqx.jstorm.dpi.bean.HdfsLSBean;
import com.cqx.jstorm.dpi.message.FastFailureMessage;
import com.cqx.jstorm.comm.spout.ISpout;
import com.cqx.jstorm.comm.util.TimeCostUtil;
import com.cqx.jstorm.dpi.utils.FastFailureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 快速失败Spout
 *
 * @author chenqixu
 */
public class FastFailureSpout extends ISpout {

    public static final String FAST_FAILURE_FIELD1 = "send";
    private static final Logger logger = LoggerFactory.getLogger(FastFailureSpout.class);
    private FastFailureBean fastFailureBean;
    private TimeCostUtil timeCostUtil;
    private FastFailureUtil fastFailureUtil;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        //解析参数
        fastFailureBean = ParamUtil.setValueByMap(conf, FastFailureBean.class);
        //参数打印
        ParamUtil.info(fastFailureBean, logger);
        //时间工具类
        timeCostUtil = new TimeCostUtil();
        //快速失败工具类
        fastFailureUtil = new FastFailureUtil(fastFailureBean.getBolt_num());
        //生成1000个任务
        for (int i = 0; i < 1000; i++) {
            HdfsLSBean hdfsLSBean = new HdfsLSBean();
            hdfsLSBean.setContent(String.format("%s-Task", i));
            fastFailureUtil.addData(hdfsLSBean);
        }
    }

    @Override
    public void nextTuple() throws Exception {
        //处理间隔
        if (timeCostUtil.tag(fastFailureBean.getSpout_next_run())) {
            fastFailureUtil.poll(new FastFailureUtil.FastFailureEmit() {
                @Override
                public void emit(FastFailureUtil.FastFailureTask fastFailureTask) {
                    //下发
                    collectorEmit((HdfsLSBean) fastFailureTask);
                }
            });
        }
    }

    private void collectorEmit(HdfsLSBean hdfsLSBean) {
        collector.emit(new Values(hdfsLSBean.getTaskName()), new FastFailureMessage(this, hdfsLSBean));
    }

    public void ack(Object object) {
        if (object instanceof FastFailureMessage) {
            FastFailureMessage fastFailureMessage = (FastFailureMessage) object;
            fastFailureUtil.ack(fastFailureMessage.getHdfsLSBean());
        } else {
            logger.warn("ack object is not instanceof FastFailureMessage. please check. object：{}", object);
        }
    }

    public void fail(Object object) {
        if (object instanceof FastFailureMessage) {
            FastFailureMessage fastFailureMessage = (FastFailureMessage) object;
            fastFailureUtil.fail(fastFailureMessage.getHdfsLSBean());
        } else {
            logger.warn("fail object is not instanceof FastFailureMessage. please check. object：{}", object);
        }
    }

    public void close() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FAST_FAILURE_FIELD1));
    }
}
