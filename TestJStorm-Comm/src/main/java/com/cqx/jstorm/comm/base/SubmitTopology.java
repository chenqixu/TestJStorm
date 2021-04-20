package com.cqx.jstorm.comm.base;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;
import com.cqx.jstorm.comm.bean.AgentBean;
import com.cqx.jstorm.comm.bean.BoltBean;
import com.cqx.jstorm.comm.bean.ReceiveBean;
import com.cqx.jstorm.comm.bean.SpoutBean;
import com.cqx.jstorm.comm.bolt.CommonBolt;
import com.cqx.jstorm.comm.spout.CommonSpout;
import com.cqx.jstorm.comm.util.*;
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
    private Map<String, Integer> topologySpoutTaskParallelismMap = new HashMap<>();
    private Map<String, Integer> topologyBoltTaskParallelismMap = new HashMap<>();
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
        argsParser.addParam("--jarpath");
        argsParser.perser(args);
        AgentBean agentBean = new AgentBean();
        agentBean.setConf(argsParser.getParamValue("--conf"));
        agentBean.setType(argsParser.getParamValue("--type"));
        agentBean.setJarpath(argsParser.getParamValue("--jarpath"));
        logger.info("agentBean：{}", agentBean);
        SubmitTopology.builder().submit(agentBean, "remote");
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
        for (SpoutBean spoutBean : appConst.getSpoutBeanList()) {
            SpoutDeclarer spoutDeclarer = builder.setSpout(spoutBean.getAliasname(),
                    new CommonSpout(spoutBean),
                    spoutBean.getParall());
            //获取Spout的自定义参数，并进行设置，然后从公共参数中移除
            Map spoutParam = (Map) appConst.getParamBean().get(spoutBean.getAliasname());
            if (spoutParam != null) {
                appConst.getParamBean().remove(spoutBean.getAliasname());
                logger.info("{} add-spoutParam：{}", spoutBean.getAliasname(), spoutParam);
                spoutDeclarer.addConfigurations(spoutParam);
            }
            topologySpoutTaskParallelismMap.put(spoutBean.getAliasname(), spoutBean.getParall());
        }
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
        for (BoltBean boltBean : appConst.getBoltBeanList()) {
            BoltDeclarer boltDeclarer = builder.setBolt(boltBean.getAliasname(),
                    new CommonBolt(boltBean),
                    boltBean.getParall());
            //获取Bolt的自定义参数，并进行设置，然后从公共参数中移除
            Map boltParam = (Map) appConst.getParamBean().get(boltBean.getAliasname());
            if (boltParam != null) {
                appConst.getParamBean().remove(boltBean.getAliasname());
                logger.info("{} add-boltParam：{}", boltBean.getAliasname(), boltParam);
                boltDeclarer.addConfigurations(boltParam);
            }
            switch (boltBean.getGroupingcode()) {
                case FIELDSGROUPING:
                    for (ReceiveBean receiveBean : boltBean.getReceiveBeanList()) {
                        if (receiveBean.getStreamId() != null) {
                            boltDeclarer.fieldsGrouping(receiveBean.getComponentId(), receiveBean.getStreamId(),
                                    new Fields(receiveBean.getFieldsgrouping_fields()));
                        } else {
                            boltDeclarer.fieldsGrouping(receiveBean.getComponentId(),
                                    new Fields(receiveBean.getFieldsgrouping_fields()));
                        }
                    }
                    break;
                case GLOBALGROUPING:
                    break;
                case SHUFFLEGROUPING:
                    for (ReceiveBean receiveBean : boltBean.getReceiveBeanList()) {
                        if (receiveBean.getStreamId() != null) {
                            boltDeclarer.shuffleGrouping(receiveBean.getComponentId(), receiveBean.getStreamId());
                        } else {
                            boltDeclarer.shuffleGrouping(receiveBean.getComponentId());
                        }
                    }
                    break;
                case LOCALORSHUFFLEGROUPING:
                    break;
                case LOCALFIRSTGROUPING:
                    for (ReceiveBean receiveBean : boltBean.getReceiveBeanList()) {
                        if (receiveBean.getStreamId() != null) {
                            boltDeclarer.localFirstGrouping(receiveBean.getComponentId(), receiveBean.getStreamId());
                        } else {
                            boltDeclarer.localFirstGrouping(receiveBean.getComponentId());
                        }
                    }
                    break;
                case NONEGROUPING:
                    break;
                case ALLGROUPING:
                    break;
                case DIRECTGROUPING:
                    break;
                case CUSTOMGROUPING:
                    break;
                default:
                    break;
            }
            topologyBoltTaskParallelismMap.put(boltBean.getAliasname(), boltBean.getParall());
        }
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
        // 内存分配
        long worker_memory = appConst.getTopologyBean().getWorker_memory();
        // cpu权重分配
        int cpu_slotNum = appConst.getTopologyBean().getCpu_slotNum();
        // jvm参数
        String jvm_options = appConst.getTopologyBean().getJvm_options();
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
        // 平均分的
        Map<String, Integer> topologyAvgTaskParallelismMap = new HashMap<>(topologySpoutTaskParallelismMap);
        // 允许spout的并发不是worker的倍数
        // bolt的并发必须是worker的倍数
        for (Map.Entry<String, Integer> entry : topologyBoltTaskParallelismMap.entrySet()) {
            if (entry.getValue() % totleWorkNum != 0) {
                logger.warn("{} parall：{} must be a multipe of worknum：{}，please check.", entry.getKey(), entry.getValue(), totleWorkNum);
                logger.warn("Use default rules.");
                return;
            } else {
                topologyAvgTaskParallelismMap.put(entry.getKey(), entry.getValue());
            }
        }
        // 根据worker_num，spout和bolt生成一个worker list
        List<WorkerAssignment> userDefines = new ArrayList<>(totleWorkNum);
        for (int i = 0; i < totleWorkNum; i++) {
            WorkerAssignment worker = new WorkerAssignment();
            worker.setHostName(iparr[i % iparr.length]);// 强制这个worker在某台机器上
            // 某些参数可以不设置
//            worker.setJvm(jvm);//设置这个worker的jvm参数
//            worker.setJvm("-Djava.security.auth.login.config=/bi/user/cqx/conf/kafka_client_jaas.conf");
//            worker.setMem( long mem); //设置这个worker的内存大小
//            worker.setCpu( int slotNum); //设置cpu的权重大小
            if (jvm_options != null && jvm_options.length() > 0)
                worker.setJvm(jvm_options);//设置这个worker的jvm参数
            if (worker_memory > 0)
                worker.setMem(worker_memory); //设置这个worker的内存大小
            if (cpu_slotNum > 0)
                worker.setCpu(cpu_slotNum); //设置cpu的权重大小
            for (Map.Entry<String, Integer> entry : topologyAvgTaskParallelismMap.entrySet()) {
//                // 平均分算法
//                // 每个worker的并发都是平均的
//                worker.addComponent(entry.getKey(), entry.getValue() / totleWorkNum);
                // 即可平均分又可以轮询的算法
                String key = entry.getKey();
                int value = entry.getValue();
                int result = value / totleWorkNum;
                int mod = 0;
                if (result > 0) {
                    mod = value % totleWorkNum;
                    if (mod > i) {
                        logger.info("worker {}，add {}，num：{}", i, key, result + 1);
                        worker.addComponent(key, result + 1);
                    } else {
                        logger.info("worker {}，add {}，num：{}", i, key, result);
                        worker.addComponent(key, result);
                    }
                } else {
                    if (value > i) {
                        logger.info("worker {}，add {}，num：{}", i, key, 1);
                        worker.addComponent(key, 1);
                    }
                }
            }
            logger.info("worker：{} bind ip：{}", worker, iparr[i % iparr.length]);
            userDefines.add(worker);
        }
        // 设置策略
        ConfigExtension.setUserDefineAssignment(topologxyConfig, userDefines);
    }

    public SubmitTopology setAppConst(Map<Object, Object> map) throws Exception {
        // 解析yaml
        appConst = yamlParser.parserMap(map);
        return this;
    }

    public void submit(AgentBean agentBean, String type) throws Exception {
        if (appConst == null) {
            // 解析yaml配置文件
            appConst = yamlParser.parserConf(agentBean.getConf());
        }
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
                    FileUtils.builder().listFiles(agentBean.getJarpath(), "jar"));
        } else if (type.equals("local")) {
            // 本地模式提交
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(appConst.getTopologyBean().getName(), conf, builder.createTopology());
            Utils.sleep(40000);
            cluster.shutdown();
        }
    }

    public void submit(AgentBean agentBean) throws Exception {
        submit(agentBean, "remote");
    }

    public void localSubmit(AgentBean agentBean) throws Exception {
        submit(agentBean, "local");
    }

}