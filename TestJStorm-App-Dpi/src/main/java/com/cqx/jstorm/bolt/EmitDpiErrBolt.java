package com.cqx.jstorm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.jstorm.spout.EmitDpiSpout;
import com.cqx.jstorm.util.FileUtils;
import com.cqx.jstorm.util.TimeCostUtil;
import com.cqx.jstorm.util.Utils;
import com.cqx.jstorm.utils.FileLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * EmitDpiErrBolt
 * <pre>
 *     按系统时间来创建错误文件
 *     每隔一分钟判断下是否切换文件把
 *     每来一条消息，就判断下是否要切换系统时间，每台只允许有一个ErrBolt运行
 * </pre>
 *
 * @author chenqixu
 */
public class EmitDpiErrBolt extends IBolt {

    public static final String SUFFIX = ".txt";
    private static Logger logger = LoggerFactory.getLogger(EmitDpiErrBolt.class);
    private FileLocal fileLocal;
    private String errorFilePath;
    private String errorFileName;
    private TimeCostUtil timeCostUtil;

    @Override
    public void prepare(Map stormConf, TopologyContext context) throws Exception {
        logger.info("getThisComponentId：{}，getThisTaskId：{}，getThisTaskIndex：{}",
                context.getThisComponentId(), context.getThisTaskId(), context.getThisTaskIndex());
        // 从配置中获取错误文件路径
        errorFilePath = (String) stormConf.get("errorDir");
        // 生成当前时间
        errorFileName = Utils.getNow("yyyyMMdd") + SUFFIX;
        // 错误文件
        fileLocal = new FileLocal(errorFileName, FileUtils.endWith(errorFilePath));
        fileLocal.start(true);
        // 时间花费工具类
        timeCostUtil = new TimeCostUtil();
        timeCostUtil.start();
        logger.info("prepare，errorFilePath：{}，errorFileName：{}", errorFilePath, errorFileName);
    }

    @Override
    public void execute(Tuple input) throws Exception {
        if (input.getSourceStreamId().equals(EmitDpiIBolt.ERR_STREAM_ID)) {
            // 间隔1分钟判断一次是否要文件切换
            if (timeCostUtil.tag(60 * 1000)) {
                // 获取当前时间，判断是否要做切换
                String _errorFileName = Utils.getNow("yyyyMMdd") + SUFFIX;
                // 切换文件
                if (!errorFileName.equals(_errorFileName)) {
                    logger.debug("切换文件，errorFileName：{}，_errorFileName：{}", errorFileName, _errorFileName);
                    fileLocal.close();
                    fileLocal = new FileLocal(_errorFileName, FileUtils.endWith(errorFilePath));
                    fileLocal.start(true);
                } else {// 刷新缓存
                    fileLocal.flush();
                }
            }
            String values = input.getStringByField(EmitDpiSpout.VALUES);
            logger.debug("receive error data：{}，prepare to save local file. valueList：{}", values, input.getValues());
            // 写入
            fileLocal.write(values);
        }
    }

    @Override
    public void cleanup() {
        if (fileLocal != null) fileLocal.close();
    }
}
