package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.bean.FastFailureBean;
import com.cqx.jstorm.spout.FastFailureSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * 快速失败bolt
 *
 * @author chenqixu
 */
public class FastFailureBolt extends IBolt {

    private static final Logger logger = LoggerFactory.getLogger(FastFailureBolt.class);
    private FastFailureThread fastFailureThread;
    private FastFailureBean fastFailureBean;
    private long task_seq = 0L;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        //解析参数
        fastFailureBean = ParamUtil.setValueByMap(stormConf, FastFailureBean.class);
    }

    @Override
    public void execute(Tuple input) throws Exception {
        //从上游获取数据
        String task_name = input.getStringByField(FastFailureSpout.FAST_FAILURE_FIELD1);
        task_seq++;
        //判断当前是否还在运行，如果在运行就快速失败，如果不在运行就提交任务
        //非结束状态都是快速失败
        if (fastFailureThread != null && !fastFailureThread.isTerminated()) {
            //快速失败
            this.collector.fail(input);
            logger.info(String.format("【任务序号%05d】任务快速失败，由于任务：%s还在运行，所以任务：%s需要快速失败",
                    task_seq, fastFailureThread.getTask_name(), task_name));
        } else {
            //新增一个任务，任务结束会ack
            fastFailureThread = new FastFailureThread(input, fastFailureBean.getBolt_sleep());
            fastFailureThread.start();
        }
    }

    private void commit(Tuple input) {
        //ack
        this.collector.ack(input);
        logger.info("ack {}", input);
    }

    class FastFailureThread extends Thread {
        private int sleep_time;
        private String task_name;
        private Tuple input;
        private Random random = new Random();

        FastFailureThread(Tuple input, int sleep_time) {
            //从上游获取数据
            this.task_name = input.getStringByField(FastFailureSpout.FAST_FAILURE_FIELD1);
            //用于ack
            this.input = input;
            //随机休眠的基础值
            this.sleep_time = sleep_time;
            logger.info("任务启动：{}", task_name);
        }

        public void run() {
            //随机休眠，然后打印
            int sleep = random.nextInt(sleep_time);
            SleepUtil.sleepMilliSecond(sleep);
            logger.info("任务完成：{}，随机休眠{}毫秒", task_name, sleep);
            commit(this.input);
        }

        String getTask_name() {
            return task_name;
        }

        boolean isTerminated() {
            return getState().equals(Thread.State.TERMINATED);
        }
    }
}
