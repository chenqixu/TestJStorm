package com.cqx.jstorm.test;

import backtype.storm.task.TopologyContext;
import com.cqx.jstorm.spout.ISpout;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.YamlParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TestSpout
 *
 * @author chenqixu
 */
public class TestSpout extends TestBase {
    protected String conf = getResourceClassPath("config.local.yaml");
    protected AppConst appConst;
    protected ISpout iSpout;
    protected TopologyContext context;
    protected Map stormConf;
    protected TestSpoutOutputCollector collector;

    public void prepare(String conf) throws IOException {
        // 解析配置
        YamlParser yamlParser = YamlParser.builder();
        appConst = yamlParser.parserConf(conf);
        context = TestTopologyContext.builder(appConst.getParamBean());
        stormConf = new HashMap();
        yamlParser.setConf(stormConf, appConst);
        collector = TestSpoutOutputCollector.build();
        if (iSpout != null) {
            iSpout.setTest(true);
            iSpout.setCollector(collector);
            iSpout.setContext(context);
        }
    }

    public Object pollMessage() {
        return collector.pollMessage();
    }

    public List<Object> pollTuple() {
        return collector.pollTuple();
    }
}
