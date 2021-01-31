package com.cqx.jstorm.dpi.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.dpi.bean.DpiFile;
import com.cqx.jstorm.dpi.bean.TypeDef;
import com.cqx.jstorm.dpi.message.EmitDpiMessageId;
import com.cqx.jstorm.comm.spout.ISpout;
import com.cqx.jstorm.comm.util.AppConst;
import com.cqx.jstorm.comm.util.TimeCostUtil;
import com.cqx.jstorm.comm.util.Utils;
import com.cqx.jstorm.dpi.utils.DpiFileUtil;
import com.cqx.jstorm.dpi.utils.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * EmitDpiSpout
 * <pre>
 *     扫描目录
 *     如果有文件，就吐给下游
 *     1个扫描，3个下游
 * </pre>
 *
 * @author chenqixu
 */
public class EmitDpiSpout extends ISpout {

    public static final String SOURDIR = "sourDir";
    public static final String BACKDIR = "backDir";
    public static final String TMPDIR = "tempDir";
    public static final String KEYWORD = "keyword";
    public static final String VALUES = "values";
    public static final int MAXSCANQUEUENUM = 0;
    // 监控
    private static final String scanCounter = "scanCounter";
    private static final String sendCounter = "sendCounter";
    public static int MAXQUEUENUM = 2;
    public static long SCANINTERVAL = 1 * 60 * 1000;// 默认扫描间隔1分钟
    private static Logger logger = LoggerFactory.getLogger(EmitDpiSpout.class);
    private DpiFileUtil dpiFileUtil;
    private Map<String, ConcurrentHashMap<String, String>> queueMap;
    private BlockingQueue<DpiFile> scanQueue;
    private int boltNum;
    private List<TypeDef> typeDefList;
    private String sourDir;
    private String backDir;
    private String tempDir;
    private String nameSeparator = "_";
    private String dateLocal = "3";
    private String endwith = ".txt";
    private Map<String, DpiFile> copyFromScanQueue = new HashMap<>();
    private AtomicBoolean isFirstCopy = new AtomicBoolean(true);
    private TimeCostUtil scanTimeCostUtil = new TimeCostUtil();
    private TimeCostUtil logStatusTimeCostUtil = new TimeCostUtil();
    private ScanUtil scanUtil;

    @Override
    public void open(Map conf, TopologyContext context) throws Exception {
        dpiFileUtil = DpiFileUtil.builder();
        queueMap = new HashMap<>();
        scanQueue = new LinkedBlockingQueue<>();
        // 获取配置
        boltNum = Integer.valueOf(conf.get("bolt_num").toString());
        sourDir = (String) conf.get(SOURDIR);
        backDir = (String) conf.get(BACKDIR);
        tempDir = (String) conf.get(TMPDIR);
        typeDefList = TypeDef.parser(conf.get(AppConst.TYPEDEFS));
        MAXQUEUENUM = Integer.valueOf((String) conf.get("maxqueuenum"));
        SCANINTERVAL = Long.valueOf((String) conf.get("scanInterval"));
        logger.info("sourDir：{}，backDir：{}，tempDir：{}，bolt_num：{}，typeDefList：{}，MAXQUEUENUM：{}，SCANINTERVAL：{}",
                sourDir, backDir, tempDir, boltNum, typeDefList, MAXQUEUENUM, SCANINTERVAL);
        // 单台几个并发，初始化队列
        for (int i = 1; i < boltNum + 1; i++) {
            queueMap.put("" + i, new ConcurrentHashMap<String, String>());
        }
        registerCounter(scanCounter);
        registerCounter(sendCounter);
        // 初始化扫描工具
        scanUtil = new ScanUtil(boltNum * MAXQUEUENUM, SCANINTERVAL);
        // 清理临时文件夹
        cleanTmpFile();
        logger.info("open success.");
    }

    /**
     * 先判断上次扫描队列是否需要再次扫描，扫描队列为0且处理队列为0
     *
     * @param dealQueueSize
     * @return
     */
    private void scanOld(int dealQueueSize) throws InterruptedException {
        if (scanQueue.size() <= MAXSCANQUEUENUM && dealQueueSize <= MAXSCANQUEUENUM) {
            TimeCostUtil timeCostUtil = new TimeCostUtil();
            int noMatching = 0;
            logger.info("触发扫描，扫描队列大小：{}，处理队列大小：{}", scanQueue.size(), dealQueueSize);
            timeCostUtil.start();
            // 触发扫描
            String[] scanfiles = dpiFileUtil.listFile(sourDir);
            // 识别关键字并加入扫描队列
            List<DpiFile> sortList = new ArrayList<>();
            for (String file : scanfiles) {
                logger.debug("scanfiles file：{}", file);
                DpiFile _dpiFile = new DpiFile(file, nameSeparator, dateLocal, endwith, typeDefList);
                if (_dpiFile.getTypeDef() != null) {
                    sortList.add(_dpiFile);
                } else {
                    noMatching++;
                    logger.debug("扫描到不匹配关键字的文件：{}", _dpiFile);
                }
            }
            // 排序
            Collections.sort(sortList);
            // 加入队列
            for (DpiFile dpiFile : sortList) {
                scanQueue.put(dpiFile);
            }
            if (sortList.size() > 0) {
                logger.info("本次扫描耗时：{}，本次扫描获得文件个数：{}，加入队列个数：{}，当前扫描最小周期文件：{}，本次扫描到非关键字文件：{}",
                        timeCostUtil.stopAndGet(), sortList.size(), scanQueue.size(), sortList.get(0), noMatching);
            }
        } else {
            logger.info("扫描队列剩余大小：{}，处理队列大小：{}", scanQueue.size(), dealQueueSize);
        }
    }

    /**
     * 判断条件，扫描队列小等于boltNum*MAXQUEUENUM，但是要避免无意义的扫描，每隔XXX就一次
     *
     * @param dealQueueSize
     * @throws InterruptedException
     */
    private void scanNew(int dealQueueSize) throws InterruptedException {
        // 判断条件，扫描队列小等于boltNum*MAXQUEUENUM，但是要避免无意义的扫描
        int scanQueueSize = scanQueue.size();
        // 是否每隔两分钟
        boolean timeTag = scanTimeCostUtil.tag(scanUtil.getScanInterval());
        // 每隔两分钟重置一次扫描机会标志
        if (timeTag) scanUtil.resetIsScan();
        // 判断是否满足扫描条件
        boolean isMustScan = scanUtil.isScan(scanQueueSize);
        if (isMustScan) {
            // 进行扫描
            TimeCostUtil timeCostUtil = new TimeCostUtil();
            int noMatching = 0;
            int addScanQueueCount = 0;
            logger.info("触发扫描，扫描队列大小：{}，处理队列大小：{}", scanQueueSize, dealQueueSize);
            timeCostUtil.start();
            // 触发扫描
            String[] scanfiles = dpiFileUtil.listFile(sourDir);
            // 识别关键字并加入扫描队列
            List<DpiFile> sortList = new ArrayList<>();
            for (String file : scanfiles) {
                logger.debug("scanfiles file：{}", file);
                DpiFile _dpiFile = new DpiFile(file, nameSeparator, dateLocal, endwith, typeDefList);
                if (_dpiFile.getTypeDef() != null) {
                    sortList.add(_dpiFile);
                } else {
                    noMatching++;
                    logger.debug("扫描到不匹配关键字的文件：{}", _dpiFile);
                }
            }
            // 没有扫描到
            if (sortList.size() == 0) {
                logger.info("本次扫描耗时：{}，本次扫描没有扫描到符合的文件，本次扫描到非关键字文件：{}",
                        timeCostUtil.stopAndGet(), noMatching);
            } else {// 有扫描到
                // 排序
                Collections.sort(sortList);
                // 是否是第一次复制扫描列表
                if (isFirstCopy.get()) {
                    // 第一次复制扫描列表，不需要比较
                    isFirstCopy.set(false);
                    // 复制 && 加入队列
                    for (DpiFile dpiFile : sortList) {
                        copyFromScanQueue.put(dpiFile.getFilename(), dpiFile);
                        scanQueue.put(dpiFile);
                        addScanQueueCount++;
                    }
                } else {// 不是第一次复制扫描列表，先拆分，然后比较，然后再生成新的复制扫描列表方便下次比较
                    Map<String, DpiFile> _copyFromScanQueue = new HashMap<>();
                    // 拆分出上次扫描未处理和本次扫描未处理
                    // 循环本次扫描对象
                    for (DpiFile dpiFile : sortList) {
                        // 新复制
                        _copyFromScanQueue.put(dpiFile.getFilename(), dpiFile);
                        // 从上次复制获取，判断是否加入扫描队列
                        DpiFile get = copyFromScanQueue.get(dpiFile.getFilename());
                        if (get == null) {
                            // 本次扫描的新文件加入扫描队列
                            scanQueue.put(dpiFile);
                            addScanQueueCount++;
                        }
                    }
                    // 新复制覆盖掉旧复制
                    copyFromScanQueue.clear();
                    copyFromScanQueue.putAll(_copyFromScanQueue);
                }
                logger.info("本次扫描耗时：{}，本次扫描获得文件个数：{}，加入队列个数：{}，当前扫描最小周期文件：{}，本次扫描到非关键字文件：{}",
                        timeCostUtil.stopAndGet(), sortList.size(), addScanQueueCount, sortList.get(0), noMatching);
            }
        }

        // 间隔5秒打印一次
        if (logStatusTimeCostUtil.tag(500)) {
            logger.info("扫描队列剩余大小：{}，处理队列大小：{}", scanQueueSize, dealQueueSize);
        }
    }

    @Override
    public void nextTuple() throws Exception {
        // 先判断上次扫描队列是否需要再次扫描，扫描队列为0且处理队列为0
        int dealQueueSize = getQueueMapSize();
        // 扫描
        scanNew(dealQueueSize);
        // 如果扫描队列有内容才派发往下游
        if (scanQueue.size() > 0) {
            // 要么按批往下发，要么一个一个往下发
            for (int i = 1; i < boltNum + 1; i++) {
                String tag = "" + i;
                ConcurrentHashMap<String, String> _tmpQueue = queueMap.get(tag);
                // 如果处理队列不满
                if (_tmpQueue.size() < MAXQUEUENUM) {
                    logger.info("{} 处理队列不满，当前队列大小：{}", tag, _tmpQueue.size());
                    DpiFile dpiFile = scanQueue.poll();
                    if (dpiFile != null) {
                        logger.info("从扫描队列获取到文件，发往下游，并加入处理队列：{}，dpiFile：{}", tag, dpiFile);
                        String fileName = dpiFile.getFilename();
                        String keyword = dpiFile.getTypeDef().getKeyWord();
                        _tmpQueue.put(fileName, keyword);
                        this.collector.emit(new Values(keyword, fileName),
                                new EmitDpiMessageId(this, tag, fileName));
                    } else {// 从扫描队列取不到文件
                        // 说明需要触发扫描，但是下游可能还没处理完成
                        logger.info("从扫描队列取不到文件，休眠50毫秒");
                        Utils.sleep(50);
                    }
                } else {// 处理队列是满的
                    logger.info("{} 处理队列是满的，休眠50毫秒", tag);
                    Utils.sleep(50);
                }
            }
        } else {
            logger.info("扫描队列大小为0，休眠500毫秒");
            Utils.sleep(500);
        }
    }

    private int getQueueMapSize() {
        int size = 0;
        synchronized (queueMap) {
            for (int i = 1; i < boltNum + 1; i++) {
                size += queueMap.get("" + i).size();
            }
        }
        return size;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(KEYWORD, VALUES));
    }

    @Override
    public void ack(Object object) {
        logger.info("ack object：{}", object);
        if (object instanceof EmitDpiMessageId) {
            EmitDpiMessageId emitDpiMessageId = (EmitDpiMessageId) object;
            String tag = emitDpiMessageId.getTag();
            String filename = emitDpiMessageId.getFilename();
            if (queueMap.get(tag) != null) {
                queueMap.get(tag).remove(filename);
                logger.info("ack queueMap.get({}).remove({})", tag, filename);
//                fileUtils.rename(sourDir, backDir, filename);
            }
        } else {
            logger.warn("ack object is not instanceof EmitDpiMessageId. please check. object：{}", object);
        }
    }

    /**
     * 启动的时候需要清理tmp文件夹
     */
    private void cleanTmpFile() {
        for (TypeDef typeDef : typeDefList) {
            String[] tmpfiles = dpiFileUtil.listFile(tempDir, typeDef.getKeyWord(), ".tmp");
            for (String tmpfile : tmpfiles) {
                DpiFileUtil.deleteFile(tempDir, tmpfile);
            }
        }
    }

}
