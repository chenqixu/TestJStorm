package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.dpi.bean.KafkaTuple;
import com.cqx.jstorm.dpi.bean.TypeDef;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.dpi.spout.EmitDpiSpout;
import com.cqx.jstorm.comm.util.AppConst;
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
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
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
    public void execute(Tuple input) throws Exception {
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
            String err_value = values + "|" + "错误内容xxxxxxxxxxxxxxx";
            this.collector.emit(ERR_STREAM_ID, new Values(err_value));
            logger.info("send error data {}", err_value);
        } else {// 发送kafka
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
            valueMap.put("uri", "1");
            valueMap.put("apply_classify", "1");
            valueMap.put("apply_name", "1");
            valueMap.put("web_classify", "1");
            valueMap.put("web_name", "1");
            valueMap.put("search_keyword", "1");
            valueMap.put("procedure_end_time", "1");
            valueMap.put("upbytes", "1");
            valueMap.put("downbytes", "1");
            this.collector.emit(KAFKA_STREAM_ID, new Values(new KafkaTuple("nmc_tb_lte_http", "13500000000", valueMap)));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields(EmitDpiSpout.VALUES));
        declarer.declareStream(ERR_STREAM_ID, new Fields(EmitDpiSpout.VALUES));
        declarer.declareStream(KAFKA_STREAM_ID, new Fields(KAFKA_FIELDS));
    }
}
