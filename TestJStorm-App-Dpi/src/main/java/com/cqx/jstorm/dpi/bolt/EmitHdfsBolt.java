package com.cqx.jstorm.dpi.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.cqx.common.utils.file.FileUtil;
import com.cqx.common.utils.ftp.FtpParamCfg;
import com.cqx.common.utils.sftp.SftpConnection;
import com.cqx.common.utils.sftp.SftpUtil;
import com.cqx.common.utils.system.SleepUtil;
import com.cqx.common.utils.system.TimeUtil;
import com.cqx.common.utils.zookeeper.ZookeeperTools;
import com.cqx.jstorm.comm.bolt.IBolt;
import com.cqx.jstorm.comm.util.AppConst;
import com.cqx.jstorm.comm.util.TimeCostUtil;
import com.cqx.jstorm.dpi.utils.FileNameFormat;
import com.cqx.jstorm.dpi.utils.GZMd5MemoryCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * EmitHdfsBolt
 *
 * @author chenqixu
 */
public class EmitHdfsBolt extends IBolt {

    private static final Logger logger = LoggerFactory.getLogger(EmitHdfsBolt.class);
    private TimeCostUtil timeCostUtil;
    private FileNameFormat fileNameFormat;
    private ZookeeperTools zookeeperTools;
    private SftpConnection sftpConnection;
    private String local_bak_path;
    private String remote_path;
    private BlockingQueue<String> uploadQ = new LinkedBlockingQueue<>();
    private Upload upload;

    @Override
    public void prepare(Map conf, TopologyContext context) throws Exception {
        logger.info("params：{}", conf);
        String extension = (String) conf.get("extension");
        String seq_zk_path = (String) conf.get("seq_zk_path");
        String zookeeper = (String) conf.get("zookeeper");
        String sftp_host = (String) conf.get("sftp_host");
        int sftp_port = Integer.valueOf((String) conf.get("sftp_port"));
        String sftp_user = (String) conf.get("sftp_user");
        String sftp_password = (String) conf.get("sftp_password");
        local_bak_path = (String) conf.get("local_bak_path");
        remote_path = (String) conf.get("remote_path");

        //替换local_bak_path中的${run_date}
        TimeUtil timeUtil = new TimeUtil();
        String run_date = timeUtil.getDateFormat("yyyyMMdd");
        local_bak_path = local_bak_path.replace("${run_date}", run_date);
        FileUtil.CreateDir(local_bak_path);

        timeCostUtil = new TimeCostUtil(true);
        zookeeperTools = ZookeeperTools.getInstance();
        zookeeperTools.init(zookeeper);
        fileNameFormat = new FileNameFormat(extension, seq_zk_path);
        FtpParamCfg ftpParamCfg = new FtpParamCfg(sftp_host, sftp_port, sftp_user, sftp_password);
        sftpConnection = SftpUtil.getSftpConnection(ftpParamCfg);
        upload = new Upload();
        new Thread(upload).start();//启动上传线程
    }

    @Override
    public void execute(Tuple input) throws Exception {
        timeCostUtil.start();
        List<String> objectList = (List<String>) input.getValueByField(AppConst.FIELDS);
        timeCostUtil.stopAndIncrementCost();
        logger.info("objectList.size：{}，IncrementCost：{}", objectList.size(), timeCostUtil.getIncrementCost());
        new Thread(new FutureTask<>(new SplitPackage(objectList, local_bak_path))).start();
    }

    @Override
    public void cleanup() {
        if (upload != null) upload.stop();
        if (zookeeperTools != null) zookeeperTools.close();
        if (sftpConnection != null) SftpUtil.closeSftpConnection(sftpConnection);
    }

    class Upload implements Runnable {
        volatile boolean flag = true;

        @Override
        public void run() {
            logger.info("============== 启动上传线程");
            while (flag) {
                String fileName;
                while ((fileName = uploadQ.poll()) != null) {
                    //判断是否退出
                    if (!flag) break;
                    //上传
                    String local = local_bak_path + fileName;
                    String remote = remote_path + fileName;
                    SftpUtil.upload(sftpConnection, local, remote);
                }
                SleepUtil.sleepMilliSecond(200);
            }
            logger.info("============== 停止上传线程");
        }

        public void stop() {
            flag = false;
        }
    }

    class SplitPackage implements Callable<String> {
        private List<String> messages;
        private String local_bak_path;//创建本地备份文件的路径

        SplitPackage(List<String> messages, String local_bak_path) {
            this.messages = messages;
            this.local_bak_path = local_bak_path;
        }

        @Override
        public String call() throws Exception {
            TimeCostUtil timeCostUtil = new TimeCostUtil();
            timeCostUtil.start();
            Date startTime = new Date();
            GZMd5MemoryCalculator gzMd5MemoryCalculator = new GZMd5MemoryCalculator();
            StringBuilder sb = new StringBuilder();
            for (String msg : messages) {
                sb.append(msg);
            }
            gzMd5MemoryCalculator.write_flush(sb.toString());
            String md5 = gzMd5MemoryCalculator.digest();
            long size = gzMd5MemoryCalculator.fileSize();
            Date endTime = new Date();
            String fileName = fileNameFormat.getName(startTime, endTime, md5, messages.size(), size);
            String localBakFile = local_bak_path + fileName;
            // 构造本地文件输出
            try (OutputStream fileOut = new FileOutputStream(localBakFile)) {
                fileOut.write(gzMd5MemoryCalculator.getResultFile());
                fileOut.flush();
            } catch (Exception e) {
                throw new RuntimeException("无法创建文件:" + localBakFile, e);
            }
            // 构造本地OK文件
            try (OutputStream fileOut = new FileOutputStream(localBakFile + ".ok")) {
                fileOut.flush();
            } catch (Exception e) {
                throw new RuntimeException("无法创建文件:" + localBakFile + ".ok", e);
            }
            timeCostUtil.stop();
            logger.debug("成功新建本地备份文件：{}，耗时：{}", localBakFile, timeCostUtil.getCost());
            uploadQ.put(fileName);//加入上传队列
            return fileName;
        }
    }
}
