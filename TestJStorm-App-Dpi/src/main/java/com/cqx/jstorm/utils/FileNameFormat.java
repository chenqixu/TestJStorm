package com.cqx.jstorm.utils;

import com.cqx.common.utils.zookeeper.ZookeeperTools;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * FileNameFormat
 *
 * @author chenqixu
 */
public class FileNameFormat {

    private static final Logger logger = LoggerFactory.getLogger(FileNameFormat.class);
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private String fileNameFormat;//原始表达式。
    private DistributedAtomicLong idSeq;//信息ID递增号
    private String deviceId = "unkonw";

    public FileNameFormat(String fileNameFormat, String seqZkPath) {
        this.fileNameFormat = fileNameFormat;
        this.idSeq = ZookeeperTools.getInstance().getDistributedAtomicLong(seqZkPath);
    }

    public synchronized String getName(Date startTime, Date endTime, String md5, long records, long fileSize) throws Exception {
        //业务功能简写 文本
        String fileName = fileNameFormat;
        String seq = this.getSeqId();
        //设备唯一标识
        fileName = fileName.replace("${device_id}", deviceId);
        //信息ID号
        fileName = fileName.replace("${seq}", seq + "");
        fileName = fileName.replace("${file_start_time}", sdf.format(startTime));
        fileName = fileName.replace("${file_end_time}", sdf.format(endTime));
        fileName = fileName.replace("${md5}", md5);
        fileName = fileName.replace("${record_count}", "" + records);
        fileName = fileName.replace("${file_size}", "" + getBToKb(fileSize));
        return fileName;
    }

    /**
     * b转kb，尾数向上取整
     *
     * @param fileSize
     * @return
     */
    private long getBToKb(long fileSize) {
        long kb = fileSize / 1024;
        if (fileSize % 1024 > 0) {
            kb += 1;
        }
        return kb;
    }

    // 获取信息ID。补0 12位
    public String getSeqId() throws Exception {
        int redo_cnt = 5;
        //尝试redo_cnt次
        for (int i = 0; i < redo_cnt; i++) {
            AtomicValue<Long> result;
            try {
                result = idSeq.increment();
            } catch (Throwable e) {
                logger.error("=======捕获到异常，" + e.getMessage(), e);
                throw e;
            }
            if (result.succeeded()) {
                long seq = result.postValue();
                return String.format("%012d", seq);
            } else {
                Thread.sleep(5); // 如果更新失败，休眠5毫秒后重试
            }
        }
        throw new RuntimeException(String.format("重试%s次，依然无法获取信息ID序号.", redo_cnt));
    }
}
