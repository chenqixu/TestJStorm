package com.cqx.jstorm.jmx;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * 发布MBean到sun自带的JMX
 * <pre>
 *     1、需要在配置中加入，端口可以自行决定，不要和其他冲突即可
 *     param:
 *         topology.worker.childopts: "-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=12345"
 *     2、MBean写法
 *         需要写一个接口，命名规范为：名字+MBean
 *         再写一个实现类，命名规范为：名字
 *     示例：
 *         接口：public interface HdfsMBean
 *         实现类：public class Hdfs implements HdfsMBean
 *     3、具体使用示例：
 *         //头部定义
 *         private Hdfs hdfs;//定义实现类
 *         //在初始化方法中
 *         hdfs = new Hdfs();//初始化实现类
 *         JMXMbeanFactory.register("Hdfs", hdfs);//发布到jmx
 * </pre>
 *
 * @author chenqixu
 */
public class JMXMbeanFactory {

    public static void register(String objectname, Object obj) throws Exception {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName object = new ObjectName(objectname + "MBean:name=" + objectname);
        mBeanServer.registerMBean(obj, object);
    }
}
