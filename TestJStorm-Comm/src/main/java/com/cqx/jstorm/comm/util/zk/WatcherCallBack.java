package com.cqx.jstorm.comm.util.zk;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * WatcherCallBack
 *
 * @author chenqixu
 */
public interface WatcherCallBack {

    void execute(KeeperState state, EventType type, String path);
}
