package com.cqx.jstorm.agent;

import com.cqx.jstorm.comm.base.KillTopology;
import com.cqx.jstorm.comm.base.SubmitTopology;
import com.cqx.jstorm.comm.bean.AgentBean;
import com.cqx.jstorm.sql.util.ParserUtil;
import com.cqx.jstorm.comm.util.ArgsParser;
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
        argsParser.addParam("--jarpath");
        argsParser.perser(args);
        AgentBean agentBean = new AgentBean();
        agentBean.setConf(argsParser.getParamValue("--conf"));
        agentBean.setType(argsParser.getParamValue("--type"));
        agentBean.setJarpath(argsParser.getParamValue("--jarpath"));
        logger.info("agentBean：{}", agentBean);
        // 选择执行方式
        switch (agentBean.getType()) {
            case "submit":
                // 提交任务
                SubmitTopology.builder().submit(agentBean);
                break;
            case "localsubmit":
                // 提交任务
                SubmitTopology.builder().localSubmit(agentBean);
                break;
            case "kill":
                KillTopology.builder().kill(agentBean);
                break;
            default:
                break;
        }
    }

    public void sqlRun(String source, String sink, String exec_sql) throws Exception {
        ParserUtil parserUtil = new ParserUtil();
        parserUtil.exec(source);
        parserUtil.exec(sink);
        parserUtil.exec(exec_sql);

        String[] args = new String[]{"--conf", "null",
                "--type", "remote",
                "--jarpath", "D:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
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
        // 远程提交任务
        SubmitTopology.builder().setAppConst(parserUtil.getYaml()).submit(agentBean);
    }
}
