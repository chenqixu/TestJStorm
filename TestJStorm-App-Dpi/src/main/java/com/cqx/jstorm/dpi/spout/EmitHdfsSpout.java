package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.cqx.common.utils.file.FileResult;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.hdfs.HdfsBean;
import com.cqx.common.utils.hdfs.HdfsTool;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.jstorm.dpi.mbean.Hdfs;
import com.cqx.jstorm.comm.jmx.JMXMbeanFactory;
import com.cqx.jstorm.comm.spout.ISpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 读取HDFS，按数据包往下传
 *
 * @author chenqixu
 */
public class EmitHdfsSpout extends ISpout {

    private static final Logger logger = LoggerFactory.getLogger(EmitHdfsSpout.class);
    private HdfsTool hdfsTool;
    private String hdfsFilePath;
    private int max_line;
    private boolean isExec = false;
    private Hdfs hdfs;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        logger.info("params：{}", conf);
        String hadoop_conf = (String) conf.get("hadoop_conf");
        hdfsFilePath = (String) conf.get("hdfsFilePath");
        max_line = Integer.valueOf((String) conf.get("max_line"));

        hdfsTool = new HdfsTool(hadoop_conf, new HdfsBean());
        //发布到jmx
        hdfs = new Hdfs();
        JMXMbeanFactory.register("Hdfs", hdfs);
    }

    @Override
    public void nextTuple() throws Exception {
        if (!isExec) {
            FileUtil fileUtil = new FileUtil();
            try {
                fileUtil.setReader(hdfsTool.openFile(hdfsFilePath));
                FileResult<String> fileResult = new FileResult<String>() {
                    int cnt = 0;

                    @Override
                    public void run(String content) throws IOException {
                        //每x条提交一次
                        cnt++;
                        addFileresult(content);
                        if (cnt == max_line) {
                            //拷贝
                            List<String> cp_contents = new ArrayList<>(getFileresult());
                            //提交
                            emit(cp_contents);
                            count("submit");//指标
                            //清0
                            cnt = 0;
                            clearFileresult();
                        }
                        count("read");//指标
                    }
                };
                fileUtil.read(fileResult);
                //处理没有被提交的
                if (fileResult.getFileresult().size() > 0) {
                    emit(fileResult.getFileresult());
                }
                logger.info("============== read：{}，submit：{}", fileResult.getCount("read"), fileResult.getCount("submit"));
            } finally {
                fileUtil.closeRead();
            }
            isExec = true;
        }
        SleepUtil.sleepMilliSecond(5000);
    }

    private void emit(List<String> msgList) {
        logger.info("emit msgList，size：{}", msgList.size());
        hdfs.add(msgList.size());
        this.collector.emit(new Values(msgList));
    }

    @Override
    public void close() {
        if (hdfsTool != null) {
            try {
                hdfsTool.closeFileSystem();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
