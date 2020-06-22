package com.cqx.jstorm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.cqx.common.utils.param.ParamUtil;
import com.cqx.jstorm.bean.FastFailureBean;
import com.cqx.jstorm.message.EmitDpiMessageId;
import com.cqx.jstorm.util.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 快速失败Spout
 *
 * @author chenqixu
 */
public class FastFailureSpout extends ISpout {

    public static final String FAST_FAILURE_FIELD1 = "send";
    private static final Logger logger = LoggerFactory.getLogger(FastFailureSpout.class);
    private BlockingQueue<String> dataQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<String> failureQueue = new LinkedBlockingQueue<>();
    private ConcurrentHashMap<String, String> runningQueue = new ConcurrentHashMap<>();
    private FastFailureBean fastFailureBean;
    private TimeCostUtil timeCostUtil;
    private long task_seq = 0L;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        //解析参数
        fastFailureBean = ParamUtil.setValueByMap(conf, FastFailureBean.class);
        //参数打印
        ParamUtil.info(fastFailureBean, logger);
        //生成1000个任务
        for (int i = 0; i < 1000; i++) {
            dataQueue.put(String.format("%s-Task", i));
        }
        timeCostUtil = new TimeCostUtil();
    }

    @Override
    public void nextTuple() throws Exception {
        //处理间隔
        if (timeCostUtil.tag(fastFailureBean.getSpout_next_run())) {
            //运行队列大小小于下游并发个数，就可以继续派发
            int can_run = fastFailureBean.getBolt_num() - runningQueue.size();
            if (can_run > 0) {
                int exec = 0;
                String task_name;
                task_seq++;
                //从失败队列获取任务
                while (exec < can_run && (task_name = failureQueue.poll()) != null) {
                    //下发
                    this.collector.emit(new Values(task_name),
                            new EmitDpiMessageId(this, "", task_name));
                    exec++;
                    //提交到运行队列
                    runningQueue.put(task_name, task_name);
                    logger.info(String.format("【任务序号%05d】从失败队列获取任务：%s，并提交，当前失败队列大小%s",
                            task_seq, task_name, failureQueue.size()));
                }
                //从任务队列获取任务
                while (exec < can_run && (task_name = dataQueue.poll()) != null) {
                    //下发
                    this.collector.emit(new Values(task_name),
                            new EmitDpiMessageId(this, "", task_name));
                    exec++;
                    //提交到运行队列
                    runningQueue.put(task_name, task_name);
                    logger.info(String.format("【任务序号%05d】从任务队列获取任务：%s，并提交，当前任务队列大小%s",
                            task_seq, task_name, dataQueue.size()));
                }
                if (exec > 0) {
                    logger.info(String.format("【任务序号%05d】结束本轮派发任务，共派发了%s个任务",
                            task_seq, exec));
                } else {
                    task_seq--;
                }
            } else {
                logger.info("不满足任务派发条件，正在运行任务队列大小：{}，总共可运行任务队列大小：{}",
                        runningQueue.size(), fastFailureBean.getBolt_num());
            }
        }
    }

    public void ack(Object object) {
        if (object instanceof EmitDpiMessageId) {
            EmitDpiMessageId emitDpiMessageId = (EmitDpiMessageId) object;
            String filename = emitDpiMessageId.getFilename();
            if (runningQueue.get(filename) != null) {
                //从running队列移除
                runningQueue.remove(filename);
                logger.info("ACK. 从running队列移除：{}", filename);
            }
        } else {
            logger.warn("ack object is not instanceof EmitDpiMessageId. please check. object：{}", object);
        }
    }

    public void fail(Object object) {
        if (object instanceof EmitDpiMessageId) {
            EmitDpiMessageId emitDpiMessageId = (EmitDpiMessageId) object;
            String filename = emitDpiMessageId.getFilename();
            if (runningQueue.get(filename) != null) {
                //从running队列移除，并增加到failureQueue队列
                runningQueue.remove(filename);
                try {
                    failureQueue.put(filename);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
                logger.info("FAIL. 从running队列移除：{}，并增加到failureQueue队列，当前failureQueue队列大小{}",
                        filename, failureQueue.size());
            }
        } else {
            logger.warn("fail object is not instanceof EmitDpiMessageId. please check. object：{}", object);
        }
    }

    public void close() {
    }

    protected void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FAST_FAILURE_FIELD1));
    }
}
