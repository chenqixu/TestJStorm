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
        if (env != null && env.length() > 0) System.setProperty(STORM_CONF_FILE, env);
        String confFile = System.getProperty(STORM_CONF_FILE);
        System.out.println("confFile：" + confFile);
        try {
            Map conf = Utils.readStormConfig();
            client = NimbusClient.getConfiguredClient(conf);

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
                System.out.println("supervisors_size：" + clusterSummary.get_supervisors_size());
                List<SupervisorSummary> supervisorSummaryList = clusterSummary.get_supervisors();
                for (SupervisorSummary supervisorSummary : supervisorSummaryList) {
                    System.out.println(supervisorSummary.get_host());
                }
//                System.out.println("Successfully get cluster info \n" + Utils.toPrettyJsonString(clusterSummary));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
