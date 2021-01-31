package com.cqx.jstorm.connector.operator;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.connector.operator.bean.OperatorBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OperatorBolt
 * <br>
 * &nbsp;&nbsp;根据字段取值，并进行相应规则处理，然后下发
 *
 * @author chenqixu
 */
public class OperatorBolt extends IBolt {
    private static final Logger logger = LoggerFactory.getLogger(OperatorBolt.class);
    private List<OperatorBean> operatorBeans = new ArrayList<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        //获取group by字段
        List groupbys = (List) stormConf.get("groupby");
        for (Object groupby : groupbys) {
            logger.info("groupby：{}", groupby);
        }
        //获取规则
        List rules = (List) stormConf.get("rule");
        for (Object rule : rules) {
            OperatorBean operatorBean = new OperatorBean((Map) rule);
            operatorBeans.add(operatorBean);
            logger.info("rule：{}", operatorBean);
        }
        //获取数据驻留时间

    }

    @Override
    public void execute(Tuple input) throws Exception {
        //解析数据
        //获取输入字段
        //获取输出字段
        //根据group by字段从输入字段取值
        //根据输出字段，先从规则过滤一遍，然后再从输入字段补充
        //根据group by字段进行分组，Map<GroupBy, Map<String, Column>>
        //GroupByUtil
        //setGroupByField
        //setRule
        //setSplitTime
        //addValue
        //getOutPut
        //期间，根据规则进行计算
        //数据驻留时间到了就发往下游
    }
}
