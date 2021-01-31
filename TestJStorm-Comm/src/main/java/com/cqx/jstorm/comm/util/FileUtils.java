package com.cqx.jstorm.comm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * FileUtils
 *
 * @author chenqixu
 */
public class FileUtils {

    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static FileUtils builder() {
        return new FileUtils();
    }

    public static String endWith(String path) {
        if (path.endsWith("/")) return path;
        else return path + "/";
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

    public String[] listFile(String path) {
        return listFile(path, null);
    }

    public String[] listFile(String path, final String keyword) {
        File file = new File(path);
        if (file.exists() && file.isDirectory()) {
            if (keyword != null && keyword.length() > 0) {
                logger.info("listFile use keyword：{}.", keyword);
                return file.list(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.contains(keyword);
                    }
                });
            } else {
                logger.info("listFile not use keyword.");
                return file.list();
            }
        } else {
            logger.warn("path：{}，file not exists：{} or file is not Directory：{}", path, file.exists(), file.isDirectory());
        }
        return new String[0];
    }

    public void rename(String source, String dist, String filename) {
        String _source = endWith(source) + filename;
        String _dist = endWith(dist) + filename;
        File sourcefile = new File(_source);
        File distfile = new File(_dist);
        if (sourcefile.exists() && sourcefile.isFile() && !distfile.exists()) {
            boolean flag = sourcefile.renameTo(distfile);
            logger.info("_source {} [renameTo] _dist {} [result] {}", _source, _dist, flag);
            if (!flag) logger.warn("_source {}，_dist {} renameFail!", _source, _dist);
        } else {
            logger.warn("_source：{}，_dist：{}，sourcefile.exists() ：{}, sourcefile.isFile()：{} , !distfile.exists()：{}",
                    _source, _dist, sourcefile.exists(), sourcefile.isFile(), !distfile.exists());
        }
    }
}
