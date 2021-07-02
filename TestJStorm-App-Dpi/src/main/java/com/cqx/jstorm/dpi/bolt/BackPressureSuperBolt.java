package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.codahale.metrics.Meter;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.comm.metric.MetricUtils;
import com.cqx.jstorm.comm.util.IpUtils;
import com.cqx.jstorm.comm.util.zk.ZookeeperTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * BackPressureSuperBolt
 *
 * @author chenqixu
 */
public class BackPressureSuperBolt extends IBolt {
    private static final Logger logger = LoggerFactory.getLogger(BackPressureSuperBolt.class);
    private final String yes = "0";
    private ZookeeperTool zookeeperTool;
    private Meter sp;
    private Random random;
    private String zkPath = "/bp/";

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        zkPath = zkPath + IpUtils.ip + context.getThisWorkerPort();
        logger.info("zkPath：{}", zkPath);
        String zookeeper = (String) stormConf.get("zookeeper");
        ZookeeperTool.init(zookeeper, "/", null);
        zookeeperTool = ZookeeperTool.getInstance();
        if (!zookeeperTool.exists(zkPath)) {
            zookeeperTool.mkdirs(zkPath);
            zookeeperTool.setData(zkPath, yes.getBytes());
        }
        sp = MetricUtils.getMeter("meter.sp.count");
        random = new Random();
    }

    @Override
    public void execute(Tuple input) throws Exception {
        String bp = new String(zookeeperTool.getData(zkPath, false));
        if (bp.equals(yes)) {
            String now = String.valueOf(System.currentTimeMillis());
            this.collector.emit(new Values(String.valueOf(random.nextInt(3)), now));
            sp.mark();
        }
    }
}
