package com.cqx.jstorm.test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.YamlParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TestBolt
 *
 * @author chenqixu
 */
public class TestBolt {

    protected String conf = "file:///D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\config.local.yaml";
    protected AppConst appConst;
    protected IBolt iBolt;
    protected TopologyContext context;
    protected Map stormConf;
    protected OutputCollector outputCollector;

    protected void prepare(String conf) throws IOException {
        // 解析配置
        YamlParser yamlParser = YamlParser.builder();
        appConst = yamlParser.parserConf(conf);
        context = TestTopologyContext.builder(appConst.getParamBean());
        stormConf = new HashMap();
        yamlParser.setConf(stormConf, appConst);
        outputCollector = TestOutputCollector.build();
        if (iBolt != null) {
            iBolt.setTest(true);
            iBolt.setCollector(outputCollector);
            iBolt.setContext(context);
        }
    }

    protected Tuple buildTuple(String filed, Object value) {
        return TestTuple.builder().put(filed, value);
    }

    protected Tuple buildTuple(String sourceStreamId, String filed, Object value) {
        return TestTuple.builder().put(sourceStreamId, filed, value);
    }
}
