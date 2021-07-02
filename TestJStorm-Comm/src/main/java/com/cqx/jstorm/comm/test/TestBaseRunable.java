package com.cqx.jstorm.comm.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TestBaseRunable
 *
 * @author chenqixu
 */
public abstract class TestBaseRunable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TestBaseRunable.class);
    private AtomicBoolean runFlag = new AtomicBoolean(true);

    private boolean isStop() {
        return runFlag.get();
    }

    public void stop() {
        runFlag.set(false);
    }

    @Override
    public void run() {
        logger.debug("{} start.", this);
        while (isStop()) {
            try {
                exec();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        logger.debug("{} stop.", this);
    }

    protected abstract void exec() throws Exception;
}
