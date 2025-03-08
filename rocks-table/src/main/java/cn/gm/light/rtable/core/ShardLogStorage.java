package cn.gm.light.rtable.core;

import cn.gm.light.rtable.entity.LogEntry;
import org.rocksdb.ColumnFamilyHandle;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description
 * @date 2025/3/3 19:16:02
 */
public interface ShardLogStorage extends LifeCycle {

    // 追加日志条目（支持批量）
    Long append(ColumnFamilyHandle columnFamilyHandle, LogEntry[] entries);

    // 新增截断方法
    void truncateSuffix(long startIndex);

    void truncatePrefix(long lastIndex);

    CompletableFuture<Long> asyncAppend(LogEntry[] entries);

    // 读取日志条目（起始索引）
    List<LogEntry> read(long startIndex);

    // 读取日志条目（起始索引）
    List<LogEntry> scan(long startIndex, int maxSize);

    // 读取日志条目（起始索引）
    LogEntry readByIndex(long index);

    // 读取日志条目（起始索引）
    LogEntry readLastLog();

    boolean hasIndex(long l);

    Iterable<LogEntry> iterateLogs();
}
