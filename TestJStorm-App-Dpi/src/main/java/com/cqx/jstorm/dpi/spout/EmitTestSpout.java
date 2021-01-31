package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.comm.spout.ISpout;
import com.cqx.jstorm.comm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * 提交测试
 *
 * @author chenqixu
 */
public class EmitTestSpout extends ISpout {

    private static Logger logger = LoggerFactory.getLogger(EmitTestSpout.class);
    private Random random;
    private int[] cnts = {1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000};
    private int shutdown_cnt = 0;

    @Override
    public void open(Map conf, TopologyContext context) {
        logger.info("####EmitTestSpout.open");
        random = new Random();
    }

    @Override
    public void nextTuple() throws Exception {
        if (shutdown_cnt <= 50) {
            int cnt = random.nextInt(10);
            logger.info("####EmitTestSpout.cnt：{}，value：{}", cnt, cnts[cnt]);
//        this.collector.emit(new Values(cnts[cnt]));
            this.collector.emit(new Values(2000));
        }
//        logger.info("####EmitTestSpout.sleep 200");
        Utils.sleep(200);
        shutdown_cnt++;
    }

    @Override
    public void close() {
        logger.info("####{} to_cleanup", this);
    }
}
