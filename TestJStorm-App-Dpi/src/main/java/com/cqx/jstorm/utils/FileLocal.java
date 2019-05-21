package com.cqx.jstorm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * FileLocal
 *
 * @author chenqixu
 */
public class FileLocal {
    public static final String newLine = System.getProperty("line.separator");
    public static final String fileSparator = File.separator;
    public static final String writeCode = "UTF-8";
    private static Logger logger = LoggerFactory.getLogger(FileLocal.class);
    private String finalFileName;
    private File localBack;
    private BufferedWriter bw;

    public FileLocal(String fileName, String localBackPath) {
        if (localBackPath.endsWith(fileSparator)) finalFileName = localBackPath + fileName;
        else finalFileName = localBackPath + fileSparator + fileName;
    }

    public void start() throws FileNotFoundException, UnsupportedEncodingException {
        start(false);
    }

    public void start(boolean append) throws FileNotFoundException, UnsupportedEncodingException {
        localBack = new File(finalFileName);
        bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localBack, append), writeCode));
    }

    public void write(String content) throws IOException {
        if (bw != null) {
            try {
                bw.write(content + newLine);
//                logger.debug("{} write：{}", bw, content + newLine);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                throw e;
            }
        }
    }

    public void flush() throws IOException {
        if (bw != null) {
            bw.flush();
            logger.debug("{} flush", bw);
        }
    }

    public void close() {
        if (bw != null) {
            try {
                bw.close();
                bw = null;
                logger.debug("{} close，= null", bw);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
