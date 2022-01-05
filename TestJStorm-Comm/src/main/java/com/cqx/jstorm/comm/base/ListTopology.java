package com.cqx.jstorm.comm.base;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ListTopology
 *
 * @author chenqixu
 */
public class ListTopology {

    public static final String STORM_CONF_FILE = "storm.conf.file";

    public static void main(String[] args) {
        NimbusClient client = null;
        String env = System.getenv(STORM_CONF_FILE);
        if (env == null || env.trim().length() == 0) {
            System.err.println("没有设置storm.conf.file！");
            System.exit(-1);
        }
        System.setProperty(STORM_CONF_FILE, env);
        try {
            Map conf = Utils.readStormConfig();
            client = NimbusClient.getConfiguredClient(conf);
            TopologyUtils topologyUtils = new TopologyUtils(client);

            if (args.length > 0 && !StringUtils.isBlank(args[0])) {
                String topologyName = args[0];
                TopologyInfo info = client.getClient().getTopologyInfoByName(topologyName);

//                System.out.println("Successfully get topology info \n" + Utils.toPrettyJsonString(info));
                TopologySummary topologySummary = info.get_topology();
                System.out.println("name：" + topologySummary.get_name() + "，status：" + topologySummary.get_status() + "，uptimeSecs：" + topologySummary.get_uptimeSecs());
                Map<Integer, TaskSummary> taskMap = new HashMap<>();
                for (TaskSummary taskSummary : info.get_tasks()) {
                    taskMap.put(taskSummary.get_taskId(), taskSummary);
                }
                for (ComponentSummary componentSummary : info.get_components()) {
                    System.out.println("component name：" + componentSummary.get_name() + "，taskIds：" + componentSummary.get_taskIds());
                    for (int taskid : componentSummary.get_taskIds()) {
                        System.out.println("component_name：" + componentSummary.get_name() + "，taskid：" + taskid + "，host：" + taskMap.get(taskid).get_host() + "，status：" + taskMap.get(taskid).get_status() + "，uptimeSecs：" + taskMap.get(taskid).get_uptime());
                    }
                    System.out.println("#####################");
                }
            } else {
                ClusterSummary clusterSummary = client.getClient().getClusterInfo();
                // nimbus
                System.out.println(clusterSummary.get_nimbus());
                // supervisors
                for (SupervisorSummary supervisorSummary : clusterSummary.get_supervisors()) {
                    System.out.println(supervisorSummary);
                }
                // topologies
                for (TopologySummary topologySummary : clusterSummary.get_topologies()) {
                    System.out.println(topologySummary);
                    TopologyInfo topologyInfo = topologyUtils.getTopologyInfoByName(topologySummary.get_name());
                    for (ComponentSummary componentSummary : topologyInfo.get_components()) {
                        System.out.println(String.format("name：%s，taskIds：%s"
                                , componentSummary.get_name()
                                , componentSummary.get_taskIds()
                        ));
                    }
                    List<TaskSummary> taskSummaryList = topologyInfo.get_tasks();
                    for (TaskSummary taskSummary : taskSummaryList) {
                        System.out.println(String.format("taskId：%s，host：%s，port：%s，status：%s"
                                , taskSummary.get_taskId()
                                , taskSummary.get_host()
                                , taskSummary.get_port()
                                , taskSummary.get_status()
                        ));
                    }
                }
//                System.out.println("Successfully get cluster info \n" + Utils.toPrettyJsonString(clusterSummary));
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
