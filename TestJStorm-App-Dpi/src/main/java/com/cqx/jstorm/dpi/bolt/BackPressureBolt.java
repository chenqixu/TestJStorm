package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.codahale.metrics.Meter;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.comm.bean.ReceiveBean;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.comm.metric.MetricUtils;
import com.cqx.jstorm.comm.util.IpUtils;
import com.cqx.jstorm.comm.util.zk.ZookeeperTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 反压测试Bolt
 *
 * @author chenqixu
 */
public class BackPressureBolt extends IBolt {
    private static final Logger logger = LoggerFactory.getLogger(BackPressureBolt.class);
    private final String yes = "0";
    private final String no = "1";
    private LinkedBlockingQueue<String> queue;
    private ExecRunnable execRunnable;
    private Thread thread;
    private ZookeeperTool zookeeperTool;
    private Meter deal;
    private List<String> fields;
    private String zkPath = "/bp/";

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        zkPath = zkPath + IpUtils.ip + context.getThisWorkerPort();
        logger.info("zkPath：{}", zkPath);
        String zookeeper = (String) stormConf.get("zookeeper");
        ZookeeperTool.init(zookeeper, "/", null);
        zookeeperTool = ZookeeperTool.getInstance();
        queue = new LinkedBlockingQueue<>(10000);
        execRunnable = new ExecRunnable();
        thread = new Thread(execRunnable);
        thread.start();
        deal = MetricUtils.getMeter("meter.deal.count");
        for (ReceiveBean receiveBean : getReceiveBeanList()) {
            if (receiveBean.getComponentId().equals("BackPressureSuperBolt")) {
                fields = receiveBean.getOutput_fields();
                break;
            }
        }
        logger.info("fields：{}", fields);
    }

    @Override
    public void execute(Tuple input) throws Exception {
        String id = input.getStringByField(fields.get(0));
        String value = input.getStringByField(fields.get(1));
        logger.info("id： {}，value：{}", id, value);
        boolean offerRet = queue.offer(id + value, 1, TimeUnit.SECONDS);
        int qs = queue.size();
        if (qs >= 4000 || !offerRet) {
            if (new String(zookeeperTool.getData(zkPath, false)).equals(yes)) {
                logger.info("通知zk进入阻塞状态，qs：{}，offerRet：{}", qs, offerRet);
                // 通知zk进入阻塞状态
                zookeeperTool.setData(zkPath, no.getBytes());
            }
        } else {
            recovery();
        }
    }

    @Override
    public void cleanup() {
        if (execRunnable != null) execRunnable.stop();
        if (thread != null)
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
    }

    private void recovery() throws Exception {
        if (new String(zookeeperTool.getData(zkPath, false)).equals(no)) {
            logger.info("通知zk恢复");
            // 通知zk恢复
            zookeeperTool.setData(zkPath, yes.getBytes());
        }
    }

    class ExecRunnable implements Runnable {
        private volatile boolean flag = true;
        private List<String> contents = new ArrayList<>();
        private Random random = new Random();

        @Override
        public void run() {
            while (flag) {
                if (contents.size() == 2000) {
                    contents.clear();
                    deal.mark(2000);
                    int sleep = random.nextInt(3000);
                    int qs = queue.size();
                    logger.info("满足2000条处理，随机休眠：{} ms，当前队列大小：{}", sleep, qs);
                    if (qs < 4000) {
                        try {
                            recovery();
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    SleepUtil.sleepMilliSecond(sleep);
                }
                String val = queue.poll();
                if (val != null) contents.add(val);
            }
        }

        void stop() {
            flag = false;
        }
    }
}
