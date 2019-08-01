package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * EmitTimeBolt
 *
 * @author chenqixu
 */
public class EmitTimeBolt extends IBolt {

    private static final Logger logger = LoggerFactory.getLogger(EmitTimeBolt.class);
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
    private Date lastDate;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {

    }

    @Override
    public void execute(Tuple input) throws Exception {
        String value = input.getStringByField(AppConst.FIELDS);
        String _value = value.substring(0, 13);
        Date newDate = sdf.parse(_value);
        long diff = 0;
        if (lastDate == null) {
            lastDate = newDate;
        } else {
            diff = newDate.getTime() - lastDate.getTime();
            lastDate = newDate;
            if (diff == 3600000) {
                logger.info("##小时切换###########################");
            }
        }
        logger.info("value：{}，_value：{}，lastDate：{}，diff：{}", value, _value, lastDate, diff);
        Utils.sleep(20);
    }
}
