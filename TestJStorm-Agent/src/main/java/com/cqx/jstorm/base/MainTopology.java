package com.cqx.jstorm.base;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.utils.PathUtils;
import com.cqx.jstorm.bean.BoltBean;
import com.cqx.jstorm.bolt.CommonBolt;
import com.cqx.jstorm.spout.CommonSpout;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 启动入口类
 *
 * @author chenqixu
 */
public class MainTopology {

    private static final Logger logger = LoggerFactory.getLogger(MainTopology.class);
    private List<String> dirs = new ArrayList<>();

    public static MainTopology builder() {
        return new MainTopology();
    }

    public static void main(String[] args) throws Exception {
        // 设置IO临时文件
        System.setProperty("java.io.tmpdir", "D:\\user\\chenqixu\\AppData\\Local\\Temp\\");
        // 运行
        MainTopology mainTopology = MainTopology.builder();
        mainTopology.run();
        List<String> dirs = mainTopology.getDirs();
        logger.info("MainTopology1：{}", mainTopology);
        mainTopology = null;
        logger.info("MainTopology2：{}", mainTopology);
        MainTopology.close(dirs.toArray(new String[dirs.size()]));
    }

    public static void close(String[] dirs) {
        for (String dir : dirs) {
            logger.info("path：{}", dir);
            try {
                PathUtils.rmr(dir);
            } catch (IOException e) {
                logger.error("Fail to delete " + dir);
            }
        }
    }

    public void run() throws Exception {
        Yaml yaml;
        InputStream is = null;
        Map<?, ?> map;
        AppConst appConst = new AppConst();
        try {
            // 加载yaml配置文件
            yaml = new Yaml();
            is = MainTopology.class.getClassLoader().getResourceAsStream("config.yaml");
            map = yaml.loadAs(is, Map.class);
            is.close();

            // 解析
            appConst.parserParam(map);

            // 创建topology的生成器
            TopologyBuilder builder = new TopologyBuilder();
            // 创建Spout
            SpoutDeclarer spout = builder.setSpout(appConst.getSpoutBean().getName(),
                    new CommonSpout(appConst.getSpoutBean().getName()),
                    appConst.getSpoutBean().getParall());
            // 创建bolt
            for (BoltBean boltBean : appConst.getBoltBeanList()) {
                BoltDeclarer totalBolt = builder.setBolt(boltBean.getName(),
                        new CommonBolt(boltBean.getName()),
                        boltBean.getParall())
                        .shuffleGrouping(boltBean.getComponentId());
            }
            // 配置
            Config conf = new Config();
            // 允许debug
            conf.setDebug(true);
            // 表示整个topology将使用几个worker
            conf.setNumWorkers(appConst.getTopologyBean().getWorker_num());
            // 设置ack个数
            conf.setNumAckers(appConst.getTopologyBean().getAck_num());

            // 本地模式提交
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(appConst.getTopologyBean().getName(), conf, builder.createTopology());
            Utils.sleep(3000);
            cluster.shutdown();
            dirs = cluster.getLocalClusterMap().getTmpDir();
        } finally {
            if (is != null)
                is.close();
        }
    }

    public List<String> getDirs() {
        return dirs;
    }
}
