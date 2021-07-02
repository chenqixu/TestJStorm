package com.cqx.jstorm.comm.base;

import backtype.storm.command.update_topology;
import com.cqx.common.utils.file.FileUtil;

import static com.cqx.jstorm.comm.base.ListTopology.STORM_CONF_FILE;

/**
 * UpdateTopology
 *
 * @author chenqixu
 */
public class UpdateTopology {

    public static void main(String[] args) {
        String env = System.getenv(STORM_CONF_FILE);
        if (env != null && env.length() > 0) System.setProperty(STORM_CONF_FILE, env);
        String confFile = System.getProperty(STORM_CONF_FILE);
        System.out.println("confFile：" + confFile);
        if (args.length == 1) {
            String resource = args[0];
            System.out.println("resource：" + resource + "，isExists：" + FileUtil.isExists(resource));
            if (FileUtil.isExists(resource)) {
                String[] _args = {"update", "-conf", resource};
                update_topology.main(_args);
            }
        }
    }
}
