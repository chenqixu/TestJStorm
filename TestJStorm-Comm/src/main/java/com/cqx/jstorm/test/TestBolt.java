package com.cqx.jstorm.test;

import backtype.storm.generated.StreamInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.YamlParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

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
    protected TestOutputCollector outputCollector;
    protected OutputFieldsGetter outputFieldsGetter;

    public static TestBolt builder(IBolt iBolt) {
        TestBolt testBolt = new TestBolt();
        testBolt.iBolt = iBolt;
        try {
            testBolt.prepare(testBolt.conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            testBolt.iBolt.prepare(testBolt.stormConf, testBolt.context);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return testBolt;
    }

    public void prepare(String conf) throws IOException {
        // 解析配置
        YamlParser yamlParser = YamlParser.builder();
        appConst = yamlParser.parserConf(conf);
        context = TestTopologyContext.builder(appConst.getParamBean());
        stormConf = new HashMap();
        yamlParser.setConf(stormConf, appConst);
        outputCollector = TestOutputCollector.build();
        outputFieldsGetter = new OutputFieldsGetter();
        if (iBolt != null) {
            iBolt.setTest(true);
            iBolt.setContext(context);
            iBolt.declareOutputFields(outputFieldsGetter);
            outputCollector.set_fields(getFieldsDeclaration());
            iBolt.setCollector(outputCollector);
        }
    }

    public Tuple buildTuple(String filed, Object value) {
        return TestTuple.builder().put(filed, value);
    }

    public Tuple buildTuple(String sourceStreamId, String filed, Object value) {
        return TestTuple.builder().put(sourceStreamId, filed, value);
    }

    public BlockingQueue<HashMap<String, Object>> pollTupleMap(String streamId) {
        return pollStreamIdMap().get(streamId);
    }

    public HashMap<String, BlockingQueue<HashMap<String, Object>>> pollStreamIdMap() {
        return outputCollector.pollStreamIdMap();
    }

    public void execute(Tuple input) throws Exception {
        iBolt.execute(input);
    }

    private Map<String, StreamInfo> getFieldsDeclaration() {
        return outputFieldsGetter.getFieldsDeclaration();
    }

    public void cleanup() {
        iBolt.cleanup();
    }
}
