package com.cqx.jstorm.comm.spout;

import com.cqx.jstorm.dpi.bean.DpiFile;
import com.cqx.jstorm.dpi.bean.TypeDef;
import com.cqx.jstorm.dpi.spout.EmitDpiSpout;
import com.cqx.jstorm.comm.test.TestSpout;
import com.cqx.jstorm.comm.test.TestTuple;
import com.cqx.jstorm.comm.util.AppConst;
import com.cqx.jstorm.comm.util.Utils;
import com.cqx.jstorm.dpi.utils.DpiFileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class EmitDpiSpoutTest extends TestSpout {

    private static Logger logger = LoggerFactory.getLogger(EmitDpiSpoutTest.class);
    private Random random = new Random();
    private String path = "";

    @Before
    public void setUp() throws Exception {
        // spout初始化
        iSpout = new EmitDpiSpout();
        conf = getResourceClassPath("config.local.s1mme.yaml");
        super.prepare(conf);
        iSpout.open(stormConf, context);
        path = (String) stormConf.get("sourDir");
    }

    @After
    public void tearDown() throws Exception {
        iSpout.close();
    }

    @Test
    public void nextTuple() throws Exception {
        int boltNum = Integer.valueOf(stormConf.get("bolt_num").toString());
        for (int i = 0; i < boltNum; i++) {
            ack(i);
        }
        int i = 0;
        while (i < 1000) {
            iSpout.nextTuple();
            i++;
            Utils.sleep(1);
        }
    }

    public void ack(int seq) {
        logger.info("启动ack：{}", seq);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    TestTuple tuple = pollTuple();
                    if (tuple != null) {
                        logger.info("ack receive tuple：{}，messageId：{}", tuple, tuple.getMessageId());
                        // 假装一个文件处理1200毫秒
//                        int dealtime = random.nextInt(3);
//                        if (dealtime < 1) dealtime = 1;
//                        Utils.sleep(dealtime * 1000);
                        String filename = tuple.getStringByField("values");
                        DpiFileUtil.deleteFile(path, filename);
                        int dealtime = 1200;
                        Utils.sleep(dealtime);
                        logger.info("文件处理了：{} 秒", dealtime);
                        iSpout.ack(tuple.getMessageId());
                    }
                }
            }
        }).start();
    }

    @Test
    public void dpiFileTest() {
        // lte
//        String filename = "LTE_S1UHTTP_008388787002_20190411080100.txt";
        // gn
        String filename = "Uar_74_21_im_session_60_20190603_140600_20190603_140659.txt";
        String nameSeparator = "_";
        String dateLocal = "8,9";
        String endwith = ".txt";
        List<TypeDef> typeDefList = TypeDef.parser(stormConf.get(AppConst.TYPEDEFS));
        DpiFile _dpiFile = new DpiFile(filename, nameSeparator, dateLocal, endwith, typeDefList);
        if (_dpiFile.getTypeDef() != null) {
            logger.info("1：{}", _dpiFile);
        } else {
            logger.info("2：{}", _dpiFile);
        }
    }
}