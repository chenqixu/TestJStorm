package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.bean.KafkaTuple;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 提交测试
 *
 * @author chenqixu
 */
public class EmitTestBolt extends IBolt {

    private static final Logger logger = LoggerFactory.getLogger(EmitTestBolt.class);
    private Random random;
    private String[] source = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",", -1);
    private String topic;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        logger.info("####EmitTestBolt.prepare");
        random = new Random();
        topic = (String) stormConf.get("topic");
    }

    @Override
    public void execute(Tuple input) {
        TimeCostUtil timeCostUtil = new TimeCostUtil();
        timeCostUtil.start();
        int cnt = input.getIntegerByField(AppConst.FIELDS) * 10;
        logger.info("####EmitTestBolt.recive execute，input：{}", cnt);
        for (int i = 0; i < cnt; i++) {
            // 随机产生1个字符串
            String value = nextStr(15);
            this.collector.emit(new Values(getKafkaValue(value)));
        }
        long cost = timeCostUtil.stopAndGet();
        logger.info("cnt：{}，处理了：{}", cnt, cost);
    }

    private KafkaTuple getKafkaValue(String value) {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("city_1", "1");
        valueMap.put("imsi", "1");
        valueMap.put("imei", "1");
        valueMap.put("msisdn", "13500000000");
        valueMap.put("tac", "1");
        valueMap.put("eci", "1");
        valueMap.put("rat", "1");
        valueMap.put("procedure_start_time", "1");
        valueMap.put("app_class", "1");
        valueMap.put("host", "1");
        valueMap.put("uri", value);
        valueMap.put("apply_classify", "1");
        valueMap.put("apply_name", "1");
        valueMap.put("web_classify", "1");
        valueMap.put("web_name", "1");
        valueMap.put("search_keyword", "1");
        valueMap.put("procedure_end_time", "1");
        valueMap.put("upbytes", "1");
        valueMap.put("downbytes", "1");
        return new KafkaTuple(topic, "13500000000", valueMap);
    }

    private String nextStr(int cnt) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < cnt; i++) {
            int index = random.nextInt(25);
            sb.append(source[index]);
        }
        return sb.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AppConst.FIELDS));
    }

    @Override
    public void cleanup() {
        logger.info("####{} to cleanup", this);
    }
}
