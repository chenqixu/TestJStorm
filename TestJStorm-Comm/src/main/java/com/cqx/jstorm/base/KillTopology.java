package com.cqx.jstorm.base;

import backtype.storm.generated.KillOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.cqx.jstorm.bean.AgentBean;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.YamlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * KillTopology
 *
 * @author chenqixu
 */
public class KillTopology {

    private static Logger logger = LoggerFactory.getLogger(KillTopology.class);
    private YamlParser yamlParser = YamlParser.builder();

    private KillTopology() {
    }

    public static KillTopology builder() {
        return new KillTopology();
    }

    /**
     * 参考backtype.storm.command.kill_topology
     *
     * @param agentBean
     */
    public void kill(AgentBean agentBean) {
        NimbusClient client = null;
        try {
            // 解析yaml配置文件
            AppConst appConst = YamlParser.builder().parserConf(agentBean.getConf());

            Map conf = Utils.readStormConfig();
            // 设置参数
            yamlParser.setConf(conf, appConst);
            client = NimbusClient.getConfiguredClient(conf);
            // kill参数
            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(5);
            // kill topology
            client.getClient().killTopologyWithOpts(appConst.getTopologyBean().getName(), killOpts);
            logger.info("Successfully submit command kill {}", appConst.getTopologyBean().getName());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
