package com.cqx.jstorm.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileUtilsTest {

    private FileUtils fileUtils;

    @Before
    public void setUp() throws Exception {
        fileUtils = new FileUtils();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void listFile() {
        String path = "d:\\tmp\\logs\\";
        String keyword = "bigdata";
        String[] files = fileUtils.listFile(path, keyword);
        for (String file : files) {
            System.out.println(file);
        }
    }
}