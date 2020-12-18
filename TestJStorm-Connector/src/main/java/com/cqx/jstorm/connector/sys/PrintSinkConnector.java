package com.cqx.jstorm.connector.sys;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.bean.ReceiveBean;
import com.cqx.jstorm.bolt.IBolt;
import com.cqx.jstorm.sql.bean.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * PrintSinkConnector
 *
 * @author chenqixu
 */
public class PrintSinkConnector extends IBolt {
    private static final Logger logger = LoggerFactory.getLogger(PrintSinkConnector.class);
    private String format;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        //打印表头
        for (ReceiveBean receiveBean : getReceiveBeanList()) {
            StringBuilder formatSB = new StringBuilder("|");
            Object[] val_array = new String[receiveBean.getOutput_fields().size()];
            for (int i = 0; i < receiveBean.getOutput_fields().size(); i++) {
                formatSB.append("%-20s");
                val_array[i] = receiveBean.getOutput_fields().get(i);
                formatSB.append("|");
            }
            format = formatSB.toString();
            logger.info("{}", String.format(format, val_array));
        }
    }

    @Override
    public void execute(Tuple input) throws Exception {
        for (ReceiveBean receiveBean : getReceiveBeanList()) {
            String streamId = receiveBean.getStreamId();
            if (streamId != null) {
                if (input.getSourceStreamId().equals(streamId)) {
                    run(input, receiveBean.getOutput_fields());
                }
            } else {
                run(input, receiveBean.getOutput_fields());
            }
        }
    }

    private void run(Tuple input, List<String> output_fields) {
        //打印数据
        Object[] val_array = new Object[output_fields.size()];
        for (int i = 0; i < output_fields.size(); i++) {
            val_array[i] = ((Column) input.getValueByField(output_fields.get(i))).getValue();
        }
        logger.info("{}", String.format(format, val_array));
    }
}
