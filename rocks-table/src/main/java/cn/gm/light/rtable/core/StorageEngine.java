package cn.gm.light.rtable.core;

import cn.gm.light.rtable.core.storage.ReplicationEventListener;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.LogEntry;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description
 * @date 2025/3/7 13:10:47
 */
public interface StorageEngine {
    Long appendLog(LogEntry logEntry);

    Boolean batchPut(Kv[] kvs);

    Boolean delete(Kv kv);

    Boolean get(Kv kv);

    Boolean put(Kv kv);

    Boolean batchGet(Kv[] kvs);

    void registerReplicationListener(ReplicationEventListener replicationEventListener);

    LogStorage getLogStorage();
}
