package com.cqx.jstorm.comm.base;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift.TException;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * TopologyUtils
 *
 * @author chenqixu
 */
public class TopologyUtils implements Closeable {
    private NimbusClient client;

    public TopologyUtils(NimbusClient client) {
        this.client = client;
    }

    public TopologyUtils(String confFile) {
        Map conf = Utils.loadDefinedConf(confFile);
        client = NimbusClient.getConfiguredClient(conf);
    }

    public TopologyInfo getTopologyInfoByName(String topologyName) throws TException {
        return client.getClient().getTopologyInfoByName(topologyName);
    }

    public List<TopologySummary> getTopologies() throws TException {
        ClusterSummary clusterSummary = client.getClient().getClusterInfo();
        return clusterSummary.get_topologies();
    }

    @Override
    public void close() {
        if (client != null) client.close();
    }
}
