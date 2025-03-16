package cn.gm.light.rtable.core.storage.shard;

import cn.gm.light.rtable.core.closure.Closure;
import cn.gm.light.rtable.entity.LogEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;

// 新增分片存储抽象
public interface ShardStore {
    Long append(LogEntry[] entries);

    void truncatePrefix(long index);

    CompletableFuture<Long> asyncAppend(LogEntry[] entries);

    void appendWithCallback(LogEntry[] entries,CompletableFuture<Long> future);

    void appendWithClosure(LogEntry[] entries, Closure closure);
    List<LogEntry> read(long startIndex);
    void truncateSuffix(long startIndex);

    List<LogEntry> scan(long startIndex, int maxSize);

    LogEntry readByIndex(long index);

    LogEntry readLastLog();

    boolean hasIndex(long l);

    Iterable<LogEntry> iterateLogs();

    void markFlushIndex(Long lastMarkFlushIndex);

    long getMarkFlushIndex();

    void close();
}
