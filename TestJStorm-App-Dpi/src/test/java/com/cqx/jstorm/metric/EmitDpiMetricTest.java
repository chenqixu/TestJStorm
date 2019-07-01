package com.cqx.jstorm.metric;

import com.cqx.jstorm.bean.DpiMetricBean;
import com.cqx.jstorm.util.Utils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class EmitDpiMetricTest {

    private static final Logger logger = LoggerFactory.getLogger(EmitDpiMetricTest.class);
    private EmitDpiMetric emitDpiMetric;

    @Before
    public void setUp() throws Exception {
        emitDpiMetric = new EmitDpiMetric();
    }

    @Test
    public void start() {
        new Thread(new Runnable() {
            DpiMetricBean dpiMetricBean = EmitDpiMetric.registerTaskCount(this.toString());
            Random random = new Random();

            @Override
            public void run() {
                while (true) {
                    int next = random.nextInt(500);
                    dpiMetricBean.Increment(next);
                    logger.debug("next：{}", next);
                    Utils.sleep(100);
                }
            }
        }).start();
        new Thread(new Runnable() {
            DpiMetricBean dpiMetricBean = EmitDpiMetric.registerTaskCount(this.toString());
            Random random = new Random();

            @Override
            public void run() {
                while (true) {
                    int next = random.nextInt(500);
                    dpiMetricBean.Increment(next);
                    logger.debug("next：{}", next);
                    Utils.sleep(100);
                }
            }
        }).start();
        emitDpiMetric.start();
        Utils.sleep(5000);
    }
}