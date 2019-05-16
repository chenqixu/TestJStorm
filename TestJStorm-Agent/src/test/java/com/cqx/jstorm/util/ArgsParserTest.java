package com.cqx.jstorm.util;

import com.cqx.jstorm.bean.AgentBean;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArgsParserTest {

    private static Logger logger = LoggerFactory.getLogger(ArgsParserTest.class);

    @Test
    public void perser() {
        String[] args = new String[]{"--conf", "d:/tmp.xml"};
        ArgsParser argsParser = ArgsParser.builder();
        argsParser.addParam("--conf");
        argsParser.perser(args);
        AgentBean agentBean = new AgentBean();
        agentBean.setConf(argsParser.getParamValue("--conf"));
        logger.info("getConf：{}", agentBean.getConf());
    }
}