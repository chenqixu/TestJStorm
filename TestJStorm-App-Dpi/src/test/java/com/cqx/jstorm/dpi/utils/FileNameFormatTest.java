package com.cqx.jstorm.dpi.utils;

import com.cqx.common.utils.zookeeper.ZookeeperTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNameFormatTest {

    private static final Logger logger = LoggerFactory.getLogger(FileNameFormatTest.class);
    private FileNameFormat fileNameFormat;
    private ZookeeperTools zookeeperTools;

    @Before
    public void setUp() throws Exception {
        zookeeperTools = ZookeeperTools.getInstance();
        zookeeperTools.init("10.1.4.186:2183");
        fileNameFormat = new FileNameFormat(
                "01-${device_id}-${seq}-${file_start_time}-${file_end_time}-${record_count}-${md5}-${file_size}.txt.gz",
                "/computecenter/task_context/if_upload_iptrace_jitian/infoId");
    }

    @After
    public void tearDown() throws Exception {
        if (zookeeperTools != null) zookeeperTools.close();
    }

    @Test
    public void getName() throws Exception {
        logger.info("======getSeqId：{}", fileNameFormat.getSeqId());
    }
}