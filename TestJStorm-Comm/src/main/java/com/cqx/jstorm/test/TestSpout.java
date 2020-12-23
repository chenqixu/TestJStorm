package com.cqx.jstorm.test;

import backtype.storm.generated.StreamInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsGetter;
import com.cqx.jstorm.bean.SpoutBean;
import com.cqx.jstorm.spout.ISpout;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.YamlParser;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * TestSpout
 *
 * @author chenqixu
 */
public class TestSpout extends TestBase {
    protected String conf = null;
    protected AppConst appConst;
    protected ISpout iSpout;
    protected TopologyContext context;
    protected Map stormConf;
    protected TestSpoutOutputCollector collector;
    protected OutputFieldsGetter outputFieldsGetter;

    public void prepare(String conf) throws Exception {
        prepare(conf, null);
    }

    public void prepare(String conf, String spoutName) throws Exception {
        // 解析配置
        YamlParser yamlParser = YamlParser.builder();
        appConst = yamlParser.parserConf(conf);
        context = TestTopologyContext.builder(appConst.getParamBean());
        stormConf = new HashMap();
        yamlParser.setConf(stormConf, appConst);
        collector = TestSpoutOutputCollector.build();
        outputFieldsGetter = new OutputFieldsGetter();
        if (spoutName != null) {
            for (SpoutBean spoutBean : appConst.getSpoutBeanList()) {
                if (spoutBean.getName().equals(spoutName)) {
                    iSpout = ISpout.generate(spoutBean.getGenerateClassName());
                    iSpout.setTest(true);
                    iSpout.setCollector(collector);
                    iSpout.setContext(context);
                    iSpout.setSendBeanList(spoutBean.getSendBeanList());
                    iSpout.declareOutputFields(outputFieldsGetter);
                    collector.set_fields(getFieldsDeclaration());
                    iSpout.open(stormConf, context);
                    break;
                }
            }
        } else if (iSpout != null) {
            iSpout.setTest(true);
            iSpout.setCollector(collector);
            iSpout.setContext(context);
            iSpout.declareOutputFields(outputFieldsGetter);
            collector.set_fields(getFieldsDeclaration());
        }
    }

    public TestTuple pollTuple() {
        return collector.pollTuple();
    }

    public TestTuple pollTuple(String streamId) {
        return collector.pollTuple(streamId);
    }

    public HashMap<String, BlockingQueue<TestTuple>> getAllTuples() {
        return collector.pollStreamIdMap();
    }

    public void nextTuple() throws Exception {
        if (iSpout != null) iSpout.nextTuple();
    }

    private Map<String, StreamInfo> getFieldsDeclaration() {
        return outputFieldsGetter.getFieldsDeclaration();
    }

    public void close() {
        if (iSpout != null) iSpout.close();
    }
}
