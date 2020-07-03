package com.cqx.jstorm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 快速失败工具类
 *
 * @author chenqixu
 */
public class FastFailureUtil {
    private static final Logger logger = LoggerFactory.getLogger(FastFailureUtil.class);
    private BlockingQueue<FastFailureTask> dataQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<FastFailureTask> failureQueue = new LinkedBlockingQueue<>();
    private ConcurrentHashMap<String, FastFailureTask> runningQueue = new ConcurrentHashMap<>();
    private int maxRun = 0;
    private long task_seq = 0L;

    public FastFailureUtil(int maxRun) {
        this.maxRun = maxRun;
    }

    public void addData(FastFailureTask fastFailureTask) throws InterruptedException {
        dataQueue.put(fastFailureTask);
    }

    public void poll(FastFailureEmit fastFailureEmit) {
        FastFailureTask fastFailureTask;
        //运行队列大小小于下游并发个数，就可以继续派发
        int can_run = maxRun - runningQueue.size();
        if (can_run > 0) {
            int exec = 0;
            task_seq++;
            //从失败队列获取任务
            while (exec < can_run && (fastFailureTask = failureQueue.poll()) != null) {
                //下发
                fastFailureEmit.emit(fastFailureTask);
                exec++;
                //提交到运行队列
                String task_name = fastFailureTask.getTaskName();
                runningQueue.put(task_name, fastFailureTask);
                logger.info(String.format("【任务序号%05d】从失败队列获取任务：%s，并提交，当前失败队列大小%s",
                        task_seq, task_name, failureQueue.size()));
            }
            //从任务队列获取任务
            while (exec < can_run && (fastFailureTask = dataQueue.poll()) != null) {
                //下发
                fastFailureEmit.emit(fastFailureTask);
                exec++;
                //提交到运行队列
                String task_name = fastFailureTask.getTaskName();
                runningQueue.put(task_name, fastFailureTask);
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
                    runningQueue.size(), maxRun);
        }
    }

    public void ack(FastFailureTask fastFailureTask) {
        String task_name = fastFailureTask.getTaskName();
        if (runningQueue.get(task_name) != null) {
            //从running队列移除
            runningQueue.remove(task_name);
            logger.info("ACK. 从running队列移除：{}", task_name);
        }
    }

    public void fail(FastFailureTask fastFailureTask) {
        String task_name = fastFailureTask.getTaskName();
        if (runningQueue.get(task_name) != null) {
            //从running队列移除，并增加到failureQueue队列
            runningQueue.remove(task_name);
            try {
                failureQueue.put(fastFailureTask);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("FAIL. 从running队列移除：{}，并增加到failureQueue队列，当前failureQueue队列大小{}",
                    task_name, failureQueue.size());
        }
    }

    public interface FastFailureEmit {
        void emit(FastFailureTask fastFailureTask);
    }

    public interface FastFailureTask {
        String getTaskName();
    }
}
