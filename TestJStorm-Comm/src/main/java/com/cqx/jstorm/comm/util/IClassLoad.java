package com.cqx.jstorm.comm.util;

import java.util.Map;

/**
 * IClassLoad
 *
 * @author chenqixu
 */
public abstract class IClassLoad {
    protected Map<String, ?> conf;

    public IClassLoad(Map conf) {
        this.conf = conf;
    }

    public abstract void init();
}
