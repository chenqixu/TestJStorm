package com.cqx.jstorm.comm.util.zk;

import com.cqx.jstorm.comm.test.TestBaseRunable;
import com.cqx.jstorm.comm.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ZookeeperToolTest {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperToolTest.class);
    private final String ZK_PATH = "/bp";
    private ZookeeperTool zookeeperTool;

    @Before
    public void setUp() throws Exception {
        zookeeperTool = ZookeeperTool.getNewInstance("10.1.8.200:2183", "/", null);
    }

    @After
    public void tearDown() throws Exception {
        if (zookeeperTool != null) zookeeperTool.close();
    }

    @Test
    public void getInstance() throws Exception {
        if (!zookeeperTool.exists(ZK_PATH)) {
            zookeeperTool.mkdirs(ZK_PATH);
            zookeeperTool.setData(ZK_PATH, "0".getBytes());
            logger.info("not exists，mkdirs，setData 0");
        } else {
            byte[] data = zookeeperTool.getData(ZK_PATH, false);
            logger.info("getData：{}", new String(data));
        }
        List<Thread> threadList = new ArrayList<>();
        List<ZKRunnable> zkRunnableList = new ArrayList<>();
        // 并发个数
        final int parallel = 10;
        for (int i = 0; i < parallel; i++) {
            ZKRunnable zkr = new ZKRunnable();
            Thread t = new Thread(zkr);
            threadList.add(t);
            zkRunnableList.add(zkr);
        }
        // 启动
        for (Thread t : threadList) {
            t.start();
        }
        // 休眠
        Utils.sleep(1000);
        // 停止
        for (ZKRunnable zkr : zkRunnableList) {
            zkr.stop();
        }
        // 等待
        for (Thread t : threadList) {
            t.join();
        }
    }

    class ZKRunnable extends TestBaseRunable {

        @Override
        protected void exec() throws Exception {
            logger.info("{} setData", this);
            zookeeperTool.setData(ZK_PATH, "1".getBytes());
        }
    }
}