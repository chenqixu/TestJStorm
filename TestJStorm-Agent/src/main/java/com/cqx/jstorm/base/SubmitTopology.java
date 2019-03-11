package com.cqx.jstorm.base;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.cqx.jstorm.bean.AgentBean;
import com.cqx.jstorm.bolt.CommonBolt;
import com.cqx.jstorm.spout.CommonSpout;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.FileUtil;
import com.cqx.jstorm.util.YamlParser;

/**
 * SubmitTopology
 *
 * @author chenqixu
 */
public class SubmitTopology {

    private YamlParser yamlParser = YamlParser.builder();

    private SubmitTopology() {
    }

    public static SubmitTopology builder() {
        return new SubmitTopology();
    }

    public void submit(AgentBean agentBean) throws Exception {
        // 解析yaml配置文件
        AppConst appConst = yamlParser.parserConf(agentBean.getConf());

        // 创建topology的生成器
        TopologyBuilder builder = new TopologyBuilder();
        // 创建Spout
        builder.setSpout(appConst.getSpoutBean().getName(),
                new CommonSpout(appConst.getSpoutBean().getName()),
                appConst.getSpoutBean().getParall());
        // 创建bolt
        // shuffleGrouping：Tuples are randomly distributed across the bolt's tasks in a way such that each bolt is guaranteed to get an equal number of tuples.
        builder.setBolt(appConst.getBoltBean().getName(),
                new CommonBolt(appConst.getBoltBean().getName()),
                appConst.getBoltBean().getParall())
                .shuffleGrouping(appConst.getSpoutBean().getName());
        // 配置
        Config conf = new Config();
        // 允许debug
        conf.setDebug(true);
        // 表示整个topology将使用几个worker
        conf.setNumWorkers(appConst.getTopologyBean().getWorker_num());
        // 设置ack个数
        conf.setNumAckers(appConst.getTopologyBean().getAck_num());

        // 设置参数
        yamlParser.setConf(conf, appConst);
        // 获取指定路径下的所有jar包
        // 提交topology，远程提交模式
        StormSubmitter.submitTopology(appConst.getTopologyBean().getName(),
                conf, builder.createTopology(), null,
                FileUtil.builder().listFiles("D:\\Document\\Workspaces\\Git\\TestJStorm\\target", "jar"));
    }
}
