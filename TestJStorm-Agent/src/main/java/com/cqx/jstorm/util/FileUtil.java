package com.cqx.jstorm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * FileUtil
 *
 * @author chenqixu
 */
public class FileUtil {

    private static Logger logger = LoggerFactory.getLogger(FileUtil.class);

    private FileUtil() {
    }

    public static FileUtil builder() {
        return new FileUtil();
    }

    public List<File> listFiles(String path, String endWith) {
        File file = new File(path);
        List<File> fileList = new ArrayList<>();
        for (File tmp : file.listFiles()) {
            if (tmp.getName().endsWith(endWith)) {
                logger.info("tmp.getName()：{}", tmp.getName());
                fileList.add(tmp);
            }
        }
        return fileList;
    }
}
