package com.cqx.jstorm.comm.test;

import backtype.storm.generated.StreamInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.comm.bean.BoltBean;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.comm.util.AppConst;
import com.cqx.jstorm.comm.util.YamlParser;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * TestBolt
 *
 * @author chenqixu
 */
public class TestBolt extends TestBase {
    protected String conf = null;
    protected AppConst appConst;
    protected IBolt iBolt;
    protected TopologyContext context;
    protected Map stormConf;
    protected TestOutputCollector outputCollector;
    protected OutputFieldsGetter outputFieldsGetter;

    public void prepare(String conf) throws Exception {
        prepare(conf, null);
    }

    public void prepare(String conf, String boltName) throws Exception {
        // 解析配置
        YamlParser yamlParser = YamlParser.builder();
        appConst = yamlParser.parserConf(conf);
        context = TestTopologyContext.builder(appConst.getParamBean());
        stormConf = new HashMap();
        // bolt自己的参数设置
        if (boltName != null) {
            Map boltParam = (Map) appConst.getParamBean().get(boltName);
            if (boltParam != null) {
                appConst.getParamBean().remove(boltName);
                stormConf.putAll(boltParam);
            }
        } else if (iBolt != null) {
            String _boltName = iBolt.getClass().getSimpleName();
            Map boltParam = (Map) appConst.getParamBean().get(_boltName);
            if (boltParam != null) {
                appConst.getParamBean().remove(_boltName);
                stormConf.putAll(boltParam);
            }
        }
        // bolt设置
        yamlParser.setConf(stormConf, appConst);
        outputCollector = TestOutputCollector.build();
        outputFieldsGetter = new OutputFieldsGetter();
        if (boltName != null) {
            for (BoltBean boltBean : appConst.getBoltBeanList()) {
                if (boltBean.getName().equals(boltName)) {
                    iBolt = IBolt.generate(boltBean.getGenerateClassName());
                    iBolt.setTest(true);
                    iBolt.setContext(context);
                    iBolt.setCollector(outputCollector);
                    iBolt.setReceiveBeanList(boltBean.getReceiveBeanList());
                    iBolt.setSendBeanList(boltBean.getSendBeanList());
                    iBolt.declareOutputFields(outputFieldsGetter);
                    outputCollector.set_fields(getFieldsDeclaration());
                    iBolt.prepare(stormConf, context);
                    break;
                }
            }
        } else if (iBolt != null) {
            iBolt.setTest(true);
            iBolt.setContext(context);
            iBolt.setCollector(outputCollector);
            iBolt.declareOutputFields(outputFieldsGetter);
            outputCollector.set_fields(getFieldsDeclaration());
        }
    }

    public Tuple buildTuple(String filed, Object value) {
        return TestTuple.builder().put(filed, value);
    }

    public Tuple buildTuple(String sourceStreamId, String filed, Object value) {
        return TestTuple.builder().put(sourceStreamId, filed, value);
    }

    public TestTuple pollTuple() {
        return outputCollector.pollTuples();
    }

    public TestTuple pollTuple(String streamId) {
        return outputCollector.pollTuples(streamId);
    }

    public HashMap<String, BlockingQueue<TestTuple>> getAllTuples() {
        return outputCollector.pollStreamIdMap();
    }

    public void execute(Tuple input) throws Exception {
        if (iBolt != null) iBolt.execute(input);
    }

    private Map<String, StreamInfo> getFieldsDeclaration() {
        return outputFieldsGetter.getFieldsDeclaration();
    }

    public void cleanup() {
        if (iBolt != null) iBolt.cleanup();
    }
}
