package com.cqx.jstorm.util;

import com.cqx.jstorm.bean.AgentBean;
import com.cqx.jstorm.bean.TypeDef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class YamlParserTest {

    private static Logger logger = LoggerFactory.getLogger(YamlParserTest.class);
    private YamlParser yamlParser = YamlParser.builder();
    private AppConst appConst;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void parserConf() throws Exception {
        String[] args = new String[]{"--conf", "D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\config.local.yaml",
                "--type", "submit"};
        // 解析参数
        ArgsParser argsParser = ArgsParser.builder();
        argsParser.addParam("--conf");
        argsParser.addParam("--type");
        argsParser.perser(args);
        AgentBean agentBean = new AgentBean();
        agentBean.setConf(argsParser.getParamValue("--conf"));
        agentBean.setType(argsParser.getParamValue("--type"));
        appConst = yamlParser.parserConf(agentBean.getConf());
        Map param = appConst.getParamBean();
        logger.info("param：{}", param);
        for (TypeDef typeDef : TypeDef.parser(param.get("typedefs"))) {
            logger.info("typeDef：{}", typeDef);
        }
    }
}