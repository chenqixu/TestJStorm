package com.cqx.jstorm.test;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.YamlParser;

import java.io.IOException;
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

    protected void prepare(String conf) throws IOException {
        // 解析配置
        appConst = YamlParser.builder().parserConf(conf);
        context = TestTopologyContext.builder(appConst.getParamBean());
        stormConf = appConst.getParamBean();
    }

    protected Tuple buildTuple(String filed, String value) {
        return TestTuple.builder().put(filed, value);
    }

    protected Tuple buildTuple(String sourceStreamId, String filed, String value) {
        return TestTuple.builder().put(sourceStreamId, filed, value);
    }
}
