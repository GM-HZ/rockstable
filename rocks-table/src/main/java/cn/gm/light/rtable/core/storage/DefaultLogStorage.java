package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.LogStorage;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.utils.LongToByteArray;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 18:04:00
 */
@Slf4j
public class DefaultLogStorage implements LogStorage{
    private TRP trp;
    private final String dataDir;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong lastIndex = new AtomicLong(0);
    private RocksDB logDB;

    public DefaultLogStorage(Config config, TRP trp) {
        this.trp = trp;
        this.dataDir = initializeLogDir(config);
        try {
            initRocksDB();
            initLastIndex();
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private String initializeLogDir(Config config) {
        String serialize = trp.serialize();
        String dir = config.getDataDir() == null ? "./rtable/log/" + serialize : config.getDataDir();
        File file = new File(dir);
        if (!file.exists()) {
            boolean success = file.mkdirs();
            if (success) {
                log.warn("Created a new directory: " + dir);
            }
        }
        return dir;
    }

    private void initLastIndex() throws RocksDBException {
        try (RocksIterator it = logDB.newIterator()) {
            it.seekToLast();
            if (it.isValid()) {
                long index = LongToByteArray.bytesToLong(it.key());
                lastIndex.set(index);
            } else {
                // 确保初始状态正确
                lastIndex.set(0L);
            }
        }
    }

    private void initRocksDB() throws RocksDBException {
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        logDB = RocksDB.open(options, dataDir);
        initLastIndex();
    }

    @Override
    public Long append(LogEntry[] entries) {
        lock.lock();
        try {
            if (entries.length == 0) {
                return lastIndex.get();
            }
            WriteBatch batch = new WriteBatch();

            // 问题1：lastIndex过早被修改
            long expectedIndex = lastIndex.get() + 1;

            for (LogEntry entry : entries) {
                Long index = entry.getIndex();

                // 问题2：并发场景下lastIndex中间状态暴露
                if (index == null) {
                    lastIndex.set(expectedIndex); // 危险操作：批处理未提交即更新状态
                    entry.setIndex(expectedIndex);
                }

                if (index != null && index != expectedIndex) {
                    throw new IllegalStateException("Invalid log index sequence. Expected:"
                            + expectedIndex + " Actual:" + index + " entries:" + JSON.toJSONString(entries));
                }

                byte[] key = LongToByteArray.longToBytes(expectedIndex);
                byte[] value = JSON.toJSONBytes(entry);
                batch.put(key, value);
                expectedIndex++;
            }

            // 最终一致性更新（修复点）
            lastIndex.set(expectedIndex - 1);

            WriteOptions writeOptions = new WriteOptions();
            writeOptions.setSync(true);
            logDB.write(writeOptions, batch);
            return lastIndex.get();
        } catch (Exception e) {
            // 异常时需要重置lastIndex（新增修复）
            long validIndex = findLastValidIndex();
            lastIndex.set(validIndex);
            log.error("Failed to append log entries: {}", JSON.toJSONString(entries), e);
        } finally {
            lock.unlock();
        }
        return null;
    }

    // 新增异常恢复方法
    private long findLastValidIndex() {
        try (RocksIterator it = logDB.newIterator()) {
            it.seekToLast();
            return it.isValid() ? LongToByteArray.bytesToLong(it.key()) : 0;
        }
    }

    @Override
    public void truncateSuffix(long startIndex) {
        lock.lock();
        try {
            if (startIndex < lastIndex.get() + 1) {
                return;
            }
            byte[] startKey = LongToByteArray.longToBytes(startIndex);
            byte[] endKey = LongToByteArray.longToBytes(lastIndex.get() + 1);
            logDB.deleteRange(startKey, endKey);
            lastIndex.set(startIndex - 1);
        } catch (RocksDBException e) {
            log.error("Failed to truncate log entries:{}", startIndex, e);
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void truncatePrefix(long index) {
        lock.lock();
        try {
            byte[] startKey = LongToByteArray.longToBytes(0);
            byte[] endKey = LongToByteArray.longToBytes(index + 1);
            logDB.deleteRange(startKey, endKey);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }


    @Override
    public CompletableFuture<Long> asyncAppend(LogEntry[] entries) {
        return CompletableFuture.supplyAsync(() -> append(entries)).exceptionally(e -> {
            log.warn("Async append failed: ", e);
            return null;
        });
    }

    @Override
    public List<LogEntry> read(long startIndex) {
        if (startIndex < 0) {
            throw new IllegalArgumentException("Start index cannot be negative");
        }
        if (startIndex > lastIndex.get()) {
            throw new IllegalArgumentException("Start index exceeds last index");
        }
        lock.lock();
        try {
            // 防止整数溢出（当 startIndex + maxSize 超过 Long.MAX_VALUE 时）
            long endIndex = lastIndex.get();
            int maxSize = (int) (lastIndex.get() - startIndex + 1);

            // 使用迭代器批量读取提升性能
            return readLogEntries(startIndex, maxSize, endIndex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<LogEntry> scan(long startIndex, int maxSize) {
        if (startIndex < 0) {
            throw new IllegalArgumentException("Start index cannot be negative");
        }
        if (startIndex > lastIndex.get()) {
            throw new IllegalArgumentException("Start index exceeds last index");
        }
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Max size must be greater than 0");
        }
        lock.lock();
        try {
            // 防止整数溢出（当 startIndex + maxSize 超过 Long.MAX_VALUE 时）
            long endIndex = Math.min(startIndex + (long) maxSize - 1, lastIndex.get());
            return readLogEntries(startIndex, maxSize, endIndex);
        } finally {
            lock.unlock();
        }
    }
    private List<LogEntry> readLogEntries(long startIndex, int maxSize, long endIndex) {
        // 第一次通过迭代器批量读取
        List<LogEntry> firstPass = readByIterator(startIndex, maxSize, endIndex);

        // 二次校验读取结果
        if (!validateEntriesOrder(firstPass, startIndex)) {
            log.warn("检测到日志顺序异常，尝试重新读取");
            return readByIteratorStrict(startIndex, maxSize, endIndex);
        }
        return firstPass;
    }

    // 原始迭代器读取方法（重命名为 readByIterator）
    private List<LogEntry> readByIterator(long startIndex, int maxSize, long endIndex) {
        try (RocksIterator iterator = logDB.newIterator()) {
            List<LogEntry> result = new ArrayList<>(maxSize);
            iterator.seek(LongToByteArray.longToBytes(startIndex));
            for (int count = 0; iterator.isValid() && count < maxSize; iterator.next(), count++) {
                long currentIndex = LongToByteArray.bytesToLong(iterator.key());
                if (currentIndex > endIndex) break;
                result.add(JSON.parseObject(iterator.value(), LogEntry.class));
            }
            return result;
        }
    }

    // 新增严格顺序读取方法
    private List<LogEntry> readByIteratorStrict(long startIndex, int maxSize, long endIndex) {
        List<LogEntry> result = new ArrayList<>(maxSize);
        long expectedIndex = startIndex;

        for (int i = 0; i < maxSize && expectedIndex <= endIndex; i++, expectedIndex++) {
            LogEntry entry = readByIndex(expectedIndex); // 逐个索引读取
            if (entry == null) break;
            result.add(entry);
        }
        return result;
    }

    // 新增顺序验证方法
    private boolean validateEntriesOrder(List<LogEntry> entries, long startIndex) {
        long expectedIndex = startIndex;
        for (LogEntry entry : entries) {
            if (entry.getIndex() != expectedIndex) {
                log.error("日志顺序异常 expected:{}, actual:{}", expectedIndex, entry.getIndex());
                return false;
            }
            expectedIndex++;
        }
        return true;
    }

    @Override
    public LogEntry readByIndex(long index) {
        lock.lock();
        try {
            if (index <= 0) {
                return LogEntry.builder().index(0L).term(0L).command(null).build();
            }
            byte[] bytes = logDB.get(LongToByteArray.longToBytes(index));
            return bytes != null ? JSON.parseObject(bytes, LogEntry.class) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public LogEntry readLastLog() {
        lock.lock();
        try {
            long index = lastIndex.get();
            if (index <= 0) {
                return LogEntry.builder().index(0L).term(0L).command(null).build();
            }
            byte[] bytes = logDB.get(LongToByteArray.longToBytes(index));
            return bytes != null ? JSON.parseObject(bytes, LogEntry.class) : null;
        } catch (RocksDBException e) {
            log.error("读取日志失败", e);
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean hasIndex(long l) {
        if (l == 0) {
            return true;
        }
        LogEntry logEntry = readByIndex(l);
        return logEntry != null;
    }

    @Override
    public Iterable<LogEntry> iterateLogs() {
        return () -> {
            lock.lock();
            try {
                // 创建快照保证数据一致性
                Snapshot snapshot = logDB.getSnapshot();
                ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
                RocksIterator rocksIterator = logDB.newIterator(readOptions);
                rocksIterator.seekToFirst();

                return new Iterator<LogEntry>() {
                    private boolean closed = false;

                    @Override
                    public boolean hasNext() {
                        if (closed) return false;
                        boolean isValid = rocksIterator.isValid();
                        if (!isValid) close();
                        return isValid;
                    }

                    @Override
                    public LogEntry next() {
                        if (closed || !rocksIterator.isValid()) {
                            throw new NoSuchElementException();
                        }
                        // 从key解析index并设置到entry
                        long index = LongToByteArray.bytesToLong(rocksIterator.key());
                        LogEntry entry = JSON.parseObject(rocksIterator.value(), LogEntry.class);
                        entry.setIndex(index);
                        rocksIterator.next();
                        return entry;
                    }

                    private void close() {
                        if (!closed) {
                            rocksIterator.close();
                            logDB.releaseSnapshot(snapshot);
                            closed = true;
                        }
                    }

                    @Override
                    protected void finalize() throws Throwable {
                        close();
                    }
                };
            } finally {
                lock.unlock();
            }
        };
    }

    @Override
    public void init() {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        if (logDB != null) {
            try {
                logDB.close();
                log.info("RocksDB closed successfully.");
                logDB = null; // 防止重复关闭
            } catch (Exception e) {
                log.error("Failed to close RocksDB", e);
            }
        }
    }
}
