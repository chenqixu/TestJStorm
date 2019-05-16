package com.cqx.jstorm.spout;

import backtype.storm.task.TopologyContext;
import com.cqx.jstorm.spout.ISpout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * EmitSocketSpout
 *
 * @author chenqixu
 */
public class EmitSocketSpout extends ISpout {

    public static final String SOCKET_RECEIVE_PORT;
    public static final String SOCKET_SEND_PORT;
    public static final String FILE_BASE_PATH;
    public static final String PATH_NAME_DATA_SERVERSOCKET_EXCEPTION = "dataServersocketException";
    public static final String FIELD_PORT = "port";
    private static Properties props = new Properties();

    static {
        try {
            InputStream is = EmitSocketSpout.class.getClassLoader().getResourceAsStream("socket-conf.properties");
            if (is != null)
                props.load(is);
            else
                throw new NullPointerException("找不到配置文件！");
        } catch (IOException var1) {
            var1.printStackTrace();
            throw new RuntimeException("读取配置文件失败！");
        } catch (NullPointerException var2) {
            var2.printStackTrace();
            throw new RuntimeException(var2.getMessage() + "，读取配置文件失败！");
        }

        SOCKET_RECEIVE_PORT = props.getProperty("SOCKET_RECEIVE_PORT");
        SOCKET_SEND_PORT = props.getProperty("SOCKET_SEND_PORT");
        FILE_BASE_PATH = props.getProperty("FILE_BASE_PATH");
    }

    @Override
    protected void open(Map conf, TopologyContext context) throws Exception {
        logger.info("####open");
        logger.info("SOCKET_RECEIVE_PORT：{}，SOCKET_SEND_PORT：{}，FILE_BASE_PATH：{}", SOCKET_RECEIVE_PORT, SOCKET_SEND_PORT, FILE_BASE_PATH);
    }

    @Override
    protected void nextTuple() throws Exception {
        logger.info("####not submit nextTuple");
    }
}
