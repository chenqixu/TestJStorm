package com.cqx.jstorm.dpi.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.zip.GZIPOutputStream;

/**
 * 内存计算GZ文件的MD5逻辑
 *
 * @author chenqixu
 */
public class GZMd5MemoryCalculator {

    //先生产内存压缩文件
    private ByteArrayOutputStream outStream;
    private GZIPOutputStream gzout;
    //最终生成在内存里的文件字节
    private byte[] resultFile;

    public GZMd5MemoryCalculator() throws IOException {
        outStream = new ByteArrayOutputStream();
        gzout = new GZIPOutputStream(outStream);
    }

    private void write(byte[] msg) throws IOException {
        gzout.write(msg);
    }

    public void write_flush(String msg) throws IOException {
        try {
            write(msg.getBytes());
        } finally {
            gzout.close();
        }
    }

    public byte[] getResultFile() {
        return resultFile;
    }

    /**
     * 计算最终MD5值
     *
     * @return MD5值
     * @throws Exception 异常抛出
     */
    public String digest() throws Exception {
        String strMD5;
        try {
            //赋值
            resultFile = outStream.toByteArray();
            //内存里计算文件MD5
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] result = md5.digest(resultFile);
            strMD5 = new BigInteger(1, result).toString(16);
        } finally {
            outStream.close();
        }
        return strMD5;
    }

    /**
     * 获得最终文件大小
     *
     * @return 文件大小
     */
    public int fileSize() {
        return resultFile == null ? 0 : resultFile.length;
    }

}
