package com.cqx.jstorm.base;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;
import com.cqx.jstorm.bean.AgentBean;
import com.cqx.jstorm.bolt.CommonBolt;
import com.cqx.jstorm.spout.CommonSpout;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.ArgsParser;
import com.cqx.jstorm.util.FileUtil;
import com.cqx.jstorm.util.YamlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SubmitTopology
 *
 * @author chenqixu
 */
public class SubmitTopology {

    private static Logger logger = LoggerFactory.getLogger(SubmitTopology.class);
    private YamlParser yamlParser = YamlParser.builder();
    private Map<String, Integer> topologyTaskParallelismMap = new HashMap<>();
    private AppConst appConst;

    private SubmitTopology() {
    }

    public static SubmitTopology builder() {
        return new SubmitTopology();
    }

    /**
     * 在集群上提交，需要手工把2个jar合并在一起
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 解析参数
        ArgsParser argsParser = ArgsParser.builder();
        argsParser.addParam("--conf");
        argsParser.addParam("--type");
        argsParser.perser(args);
        AgentBean agentBean = new AgentBean();
        agentBean.setConf(argsParser.getParamValue("--conf"));
        agentBean.setType(argsParser.getParamValue("--type"));
        logger.info("agentBean：{}", agentBean);
        SubmitTopology.builder().submit(agentBean, "jar");
    }

    /**
     * 创建Spout
     *
     * @param builder
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     */
    private void addSpout(TopologyBuilder builder) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        // 创建Spout
        builder.setSpout(appConst.getSpoutBean().getName(),
                new CommonSpout(appConst.getSpoutBean().getName()),
                appConst.getSpoutBean().getParall());
        topologyTaskParallelismMap.put(appConst.getSpoutBean().getName(), appConst.getSpoutBean().getParall());
    }

    /**
     * 创建Bolt
     *
     * @param builder
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     */
    private void addBolt(TopologyBuilder builder) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        // shuffleGrouping：Tuples are randomly distributed across the bolt's tasks in a way such that each bolt is guaranteed to get an equal number of tuples.
        builder.setBolt(appConst.getBoltBean().getName(),
                new CommonBolt(appConst.getBoltBean().getName()),
                appConst.getBoltBean().getParall())
                .shuffleGrouping(appConst.getSpoutBean().getName());
        topologyTaskParallelismMap.put(appConst.getBoltBean().getName(), appConst.getBoltBean().getParall());
    }

    /**
     * 自定义worker分配
     * <pre>
     *     条件1：ip不为空
     *     条件2：worknum必须是ip的倍数
     *     条件3：spout和bolt的并发必须是worker的倍数
     *     在以上规则下，保证每个worker下的spout和bolt都是平均的，每个worker都有spout和bolt
     *     否则，使用自带的默认规则
     * </pre>
     *
     * @param topologxyConfig
     */
    private void setHostAssignmentWorkers(Map topologxyConfig) {
        // ip不为空才有分配策略
        String ips = appConst.getTopologyBean().getIp();
        if (ips == null || ips.length() == 0) {
            logger.warn("ip is null , No adaptation custom Assignment rules.");
            logger.warn("Use default rules.");
            return;
        }
        int totleWorkNum = appConst.getTopologyBean().getWorker_num();
        String[] iparr = ips.split(",", -1);
        // worknum must be a multipe of ip
        if (iparr.length == 0 || totleWorkNum % iparr.length != 0) {
            logger.warn("worknum：{} must be a multipe of ip：{}，please check.", totleWorkNum, ips);
            logger.warn("Use default rules.");
            return;
        }
        // spout和bolt的并发必须是worker的倍数
        for (Map.Entry<String, Integer> entry : topologyTaskParallelismMap.entrySet()) {
            if (entry.getValue() % totleWorkNum != 0) {
                logger.warn("{} parall：{} must be a multipe of worknum：{}，please check.", entry.getKey(), entry.getValue(), totleWorkNum);
                logger.warn("Use default rules.");
                return;
            }
        }
        // 根据worker_num，spout和bolt生成一个worker list
        List<WorkerAssignment> userDefines = new ArrayList<>(totleWorkNum);
        for (int i = 0; i < totleWorkNum; i++) {
            WorkerAssignment worker = new WorkerAssignment();
            worker.setHostName(iparr[i % iparr.length]);// 强制这个worker在某台机器上
            // 某些参数可以不设置
//            worker.setJvm(jvm);//设置这个worker的jvm参数
//            worker.setMem( long mem); //设置这个worker的内存大小
//            worker.setCpu( int slotNum); //设置cpu的权重大小
            for (Map.Entry<String, Integer> entry : topologyTaskParallelismMap.entrySet()) {
                // 每个worker的并发都是平均的
                worker.addComponent(entry.getKey(), entry.getValue() / totleWorkNum);
            }
            logger.info("worker：{} bind ip：{}", worker, iparr[i % iparr.length]);
            userDefines.add(worker);
        }
        // 设置策略
        ConfigExtension.setUserDefineAssignment(topologxyConfig, userDefines);
    }

    public void submit(AgentBean agentBean, String type) throws Exception {
        // 解析yaml配置文件
        appConst = yamlParser.parserConf(agentBean.getConf());
        // 创建topology的生成器
        TopologyBuilder builder = new TopologyBuilder();
        // 创建Spout
        addSpout(builder);
        // 创建bolt
        addBolt(builder);
        // 配置
        Config conf = new Config();
        // 允许debug
        conf.setDebug(true);
        // 表示整个topology将使用几个worker
        conf.setNumWorkers(appConst.getTopologyBean().getWorker_num());
        // 设置ack个数
        conf.setNumAckers(appConst.getTopologyBean().getAck_num());
        // 设置Worker策略
        setHostAssignmentWorkers(conf);
        // 设置参数
        yamlParser.setConf(conf, appConst);
        // 在集群上使用jstorm jar xxx.jar main-class
        if (type.equals("jar")) {
            StormSubmitter.submitTopology(appConst.getTopologyBean().getName(),
                    conf, builder.createTopology());
        } else if (type.equals("remote")) {
            // 获取指定路径下的所有jar包
            // 提交topology，远程提交模式
            StormSubmitter.submitTopology(appConst.getTopologyBean().getName(),
                    conf, builder.createTopology(), null,
                    FileUtil.builder().listFiles("D:\\Document\\Workspaces\\Git\\TestJStorm\\target", "jar"));
        }
    }

    public void submit(AgentBean agentBean) throws Exception {
        submit(agentBean, "remote");
    }

}