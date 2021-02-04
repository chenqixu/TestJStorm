package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.common.model.commit.BatchCommit;
import com.cqx.common.utils.jdbc.DBBean;
import com.cqx.common.utils.jdbc.JDBCRetryUtil;
import com.cqx.common.utils.jdbc.ParamsParserUtil;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.comm.util.AppConst;
import com.cqx.jstorm.comm.util.TimeCostUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * GetBolt
 *
 * @author chenqixu
 */
public class GetBolt extends IBolt {
    private static Logger logger = LoggerFactory.getLogger(GetBolt.class);
    private BatchCommit<Integer> batchCommit;
    private JDBCRetryUtil jdbcUtil;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        ParamsParserUtil paramsParserUtil = new ParamsParserUtil(stormConf);
        DBBean adbBean = paramsParserUtil.getBeanMap().get("adbBean");
        jdbcUtil = new JDBCRetryUtil(adbBean);

        batchCommit = new BatchCommit<>(2000, 3000, 10000,
                new BatchCommit.ICallBack<Integer>() {
                    TimeCostUtil tc = new TimeCostUtil();

                    @Override
                    public void call(List<Integer> list) {
                        tc.start();
                        List<String> sqls = new ArrayList<>();
                        //插入
//                        for (int i : list) sqls.add("insert into REALTIME_LOCATION(MSISDN) values('" + i + "')");
                        //去重
                        Set<Integer> set = new HashSet<>(list);
                        //更新
                        for (int i : set) sqls.add("update REALTIME_LOCATION set imsi=MSISDN where MSISDN='" + i + "'");
                        jdbcUtil.executeBatch(sqls);
                        logger.info("执行：{}，耗时：{}", list.size(), tc.stopAndGet());
                    }
                }
        );
    }

    @Override
    public void execute(Tuple input) throws Exception {
        int value = (Integer) input.getValueByField(AppConst.FIELDS);
        batchCommit.add(value);
    }

    @Override
    public void cleanup() {
        if (jdbcUtil != null) jdbcUtil.close();
    }
}
