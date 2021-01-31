package com.cqx.jstorm.comm.test;

import backtype.storm.generated.StreamInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsGetter;
import com.cqx.jstorm.comm.bean.SpoutBean;
import com.cqx.jstorm.comm.spout.ISpout;
import com.cqx.jstorm.comm.util.AppConst;
import com.cqx.jstorm.comm.util.YamlParser;

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
        // spout自己的参数设置
        if (spoutName != null) {
            Map spoutParam = (Map) appConst.getParamBean().get(spoutName);
            if (spoutParam != null) {
                appConst.getParamBean().remove(spoutName);
                stormConf.putAll(spoutParam);
            }
        } else if (iSpout != null) {
            String _spoutName = iSpout.getClass().getSimpleName();
            Map spoutParam = (Map) appConst.getParamBean().get(_spoutName);
            if (spoutParam != null) {
                appConst.getParamBean().remove(_spoutName);
                stormConf.putAll(spoutParam);
            }
        }
        // spout配置
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
