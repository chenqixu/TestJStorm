package com.cqx.jstorm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.cqx.jstorm.bean.DpiFile;
import com.cqx.jstorm.bean.TypeDef;
import com.cqx.jstorm.message.EmitDpiMessageId;
import com.cqx.jstorm.util.AppConst;
import com.cqx.jstorm.util.FileUtils;
import com.cqx.jstorm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

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

    public static final String KEYWORD = "keyword";
    public static final String VALUES = "values";
    public static final int MAXQUEUENUM = 50;
    public static final int MAXSCANQUEUENUM = 0;
    private static Logger logger = LoggerFactory.getLogger(EmitDpiSpout.class);
    private FileUtils fileUtils;
    private Map<String, ConcurrentHashMap<String, String>> queueMap;
    private BlockingQueue<DpiFile> scanQueue;
    private int boltNum;
    private List<TypeDef> typeDefList;
    private String sourDir;
    private String backDir;

    @Override
    protected void open(Map conf, TopologyContext context) throws Exception {
        fileUtils = new FileUtils();
        queueMap = new HashMap<>();
        scanQueue = new LinkedBlockingQueue<>();
        // 获取配置
        boltNum = Integer.valueOf(conf.get("bolt_num").toString());
        sourDir = (String) conf.get(AppConst.SOURDIR);
        backDir = (String) conf.get(AppConst.BACKDIR);
        typeDefList = TypeDef.parser(conf.get(AppConst.TYPEDEFS));
        logger.info("sourDir：{}，backDir：{}，bolt_num：{}，typeDefList：{}",
                sourDir, backDir, boltNum, typeDefList);
        // 单台几个并发，初始化队列
        for (int i = 1; i < boltNum + 1; i++) {
            queueMap.put("" + i, new ConcurrentHashMap<String, String>());
        }
        logger.info("open success.");
    }

    @Override
    protected void nextTuple() throws Exception {
        // 先判断上次扫描队列是否需要再次扫描
        if (scanQueue.size() <= MAXSCANQUEUENUM && getQueueMapSize() <= MAXSCANQUEUENUM) {
            logger.info("触发扫描，scanQueue.size()：{}", scanQueue.size());
            // 触发扫描
            for (String file : fileUtils.listFile(sourDir)) {
                for (TypeDef typeDef : typeDefList) {
                    if (file.contains(typeDef.getKeyWord())) {
                        scanQueue.put(new DpiFile(file, typeDef));
                        break;
                    }
                }
            }
        }
        // 要么按批往下发，要么一个一个往下发
        for (int i = 1; i < boltNum + 1; i++) {
            String tag = "" + i;
            ConcurrentHashMap<String, String> _tmpQueue = queueMap.get(tag);
            // 如果队列不满
            if (_tmpQueue.size() < MAXQUEUENUM) {
                DpiFile dpiFile = scanQueue.poll();
                logger.info("队列不满，tag：{}，_tmpQueue.size()：{}", tag, _tmpQueue.size());
                if (dpiFile != null) {
                    String fileName = dpiFile.getFilename();
                    String keyword = dpiFile.getTypeDef().getKeyWord();
                    logger.info("提交下游，keyword：{}，fileName：{}", keyword, fileName);
                    _tmpQueue.put(fileName, keyword);
                    this.collector.emit(new Values(keyword, fileName),
                            new EmitDpiMessageId(this, tag, fileName));
                } else {// 从扫描队列取不到文件
                    // 说明需要触发扫描，但是下游可能还没处理完成
                    Utils.sleep(50);
                }
            } else {// 队列是满的
                Utils.sleep(500);
            }
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
    protected void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(KEYWORD, VALUES));
    }

    @Override
    protected void ack(Object object) {
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
}
