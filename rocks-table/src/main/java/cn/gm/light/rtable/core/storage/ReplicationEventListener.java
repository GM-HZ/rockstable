package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.entity.LogEntry;

public interface ReplicationEventListener {
        void onLogAppend(LogEntry entry);
}
