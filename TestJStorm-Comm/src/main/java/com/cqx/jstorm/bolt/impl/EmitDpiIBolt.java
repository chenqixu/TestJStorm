package com.cqx.jstorm.bolt.impl;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.bean.TypeDef;
import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.message.KafkaTuple;
import com.cqx.jstorm.spout.impl.EmitDpiSpout;
import com.cqx.jstorm.util.AppConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * EmitDpiIBolt
 *
 * @author chenqixu
 */
public class EmitDpiIBolt extends IBolt {

    public static final String ERR_STREAM_ID = "err";
    public static final String KAFKA_STREAM_ID = "kafka";
    public static final String KAFKA_FIELDS = "KAFKATUPLE";
    private static Logger logger = LoggerFactory.getLogger(EmitDpiIBolt.class);
    private List<TypeDef> typeDefList;
    private Map<String, TypeDef> typeDefMap;
    private Random random;

    @Override
    protected void prepare(Map stormConf, TopologyContext context) throws Exception {
        logger.info("getThisComponentId：{}，getThisTaskId：{}，getThisTaskIndex：{}",
                context.getThisComponentId(), context.getThisTaskId(), context.getThisTaskIndex());
        typeDefList = TypeDef.parser(stormConf.get(AppConst.TYPEDEFS));
        logger.info("typeDefList：{}", typeDefList);
        typeDefMap = new HashMap<>();
        for (TypeDef typeDef : typeDefList) {
            typeDefMap.put(typeDef.getKeyWord(), typeDef);
        }
        logger.info("typeDefMap：{}", typeDefMap);
        random = new Random();
        logger.info("prepare success.");
    }

    @Override
    protected void execute(Tuple input) throws Exception {
        String keyword = input.getStringByField(EmitDpiSpout.KEYWORD);
        String values = input.getStringByField(EmitDpiSpout.VALUES);
        logger.info("bolt get keyword：{}，values：{}", keyword, values);
        // dpi解析发送
        // 获取返回值，并保存到本地
        logger.info("dpi parser and get value and save to local.");
        logger.info("ack：{}", input);
        // ack
        this.collector.ack(input);
        // 发送异常文件
        if (random.nextBoolean()) {
            this.collector.emit(ERR_STREAM_ID, new Values(values));
            logger.info("send error data {}", values);
        } else {// 发送kafka，一批数据
            KafkaTuple<String> kafkaTuple = new KafkaTuple<>();
            kafkaTuple.add("1");
            kafkaTuple.add("2");
            kafkaTuple.add("3");
            kafkaTuple.add("4");
            kafkaTuple.add("5");
            this.collector.emit(KAFKA_STREAM_ID, new Values(kafkaTuple));
        }
    }

    protected void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields(EmitDpiSpout.VALUES));
        declarer.declareStream(ERR_STREAM_ID, new Fields(EmitDpiSpout.VALUES));
        declarer.declareStream(KAFKA_STREAM_ID, new Fields(KAFKA_FIELDS));
    }
}
