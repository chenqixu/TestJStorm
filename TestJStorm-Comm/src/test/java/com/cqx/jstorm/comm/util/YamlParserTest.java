package com.cqx.jstorm.comm.util;

import com.cqx.jstorm.comm.bean.BoltBean;
import com.cqx.jstorm.comm.bean.SpoutBean;
import com.cqx.jstorm.comm.test.TestBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class YamlParserTest extends TestBase {

    @Test
    public void parserConf() throws Exception {
        YamlParser yamlParser = YamlParser.builder();
        AppConst appConst = yamlParser.parserConf(getResourceClassPath("config.local.yaml"));
        Map stormConf = new HashMap();
        yamlParser.setConf(stormConf, appConst);
        for (SpoutBean spoutBean : appConst.getSpoutBeanList()) {
            System.out.println(spoutBean.getName() + " getSendBeanList: " + spoutBean.getSendBeanList());
        }
        for (BoltBean boltBean : appConst.getBoltBeanList()) {
            System.out.println(boltBean.getName() + " getReceiveBeanList: " + boltBean.getReceiveBeanList());
            System.out.println(boltBean.getName() + " getSendBeanList: " + boltBean.getSendBeanList());
        }
    }
}