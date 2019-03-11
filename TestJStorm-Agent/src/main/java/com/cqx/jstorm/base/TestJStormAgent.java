package com.cqx.jstorm.base;

import com.cqx.jstorm.bean.AgentBean;
import com.cqx.jstorm.util.ArgsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestJStormAgent
 *
 * @author chenqixu
 */
public class TestJStormAgent {

    private static Logger logger = LoggerFactory.getLogger(TestJStormAgent.class);

    private TestJStormAgent() {
    }

    public static TestJStormAgent builder() {
        return new TestJStormAgent();
    }

    /**
     * <pre>
     *     参数1：--conf or -c 配置文件
     * </pre>
     *
     * @param args
     */
    public void run(String[] args) throws Exception {
        // 解析参数
        ArgsParser argsParser = ArgsParser.builder();
        argsParser.addParam("--conf");
        argsParser.addParam("--type");
        argsParser.perser(args);
        AgentBean agentBean = new AgentBean();
        agentBean.setConf(argsParser.getParamValue("--conf"));
        agentBean.setType(argsParser.getParamValue("--type"));
        logger.info("agentBean：{}", agentBean);
        // 选择执行方式
        switch (agentBean.getType()) {
            case "submit":
                // 提交任务
                SubmitTopology.builder().submit(agentBean);
                break;
            case "kill":
                KillTopology.builder().kill(agentBean);
                break;
            default:
                break;
        }
    }
}
