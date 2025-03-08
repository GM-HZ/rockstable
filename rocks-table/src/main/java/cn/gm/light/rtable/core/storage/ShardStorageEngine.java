package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.LogStorage;
import cn.gm.light.rtable.core.StorageEngine;
import cn.gm.light.rtable.core.TrpNode;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.storage.shard.ShardStore;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.utils.Pair;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description 默认存储引擎实现
 * 分片存储引擎
 * 1. 数据分片存储, 在每一个分片内部，实现wal分片，mem分片，一个wal对应一个mem，降低锁的压力（wal分片采用的是rocksdb的分列实现的）
 * 2. 采用wal，mem ，rocksdb 混合存储
 * 3. 数据复制，实现的最终数据一致性
 * 4. 数据恢复，采用异步刷盘，在刷盘的时候，使用锁和cas操作当前mem为不可变mem，记录最后同步的数据的日志id，后续依据id进行恢复，直接读取到内存中，保证数据一致性
 * @date 2025/3/7 13:10:47
 */

@Slf4j
public class ShardStorageEngine implements StorageEngine {
    private final List<ReplicationEventListener> replicationListeners = new CopyOnWriteArrayList<>();
    private final DefaultDataStorage dataStorage;
    private final DefaultShardLogStorage shardLogStorage;
    private final TRP trp;
    private final TrpNode trpNode;
    private final Config config;
    // 布隆过滤器
    private final BloomFilter<byte[]> bloomFilter;
    private final int bufferSize = 64 * 1024;
    // LRU 缓存
    private final LinkedHashMap<byte[], byte[]> lruCache;
    private final int cacheSize =  64 * 1024;
    private final int shardCount = Runtime.getRuntime().availableProcessors() * 2;
    private final int maxMemory = 256 * 1024 * 1024 * shardCount; // 预设内存阈值
    // 新增内存分片锁优化
    private ReentrantLock[] shardLocks;  // 每个分片独立锁

    // 在类定义中添加列族管理
    private final ColumnFamilyHandle[] shardCfHandles = new ColumnFamilyHandle[shardCount];
    private final ColumnFamilyDescriptor[] shardCfDescriptors = new ColumnFamilyDescriptor[shardCount];

    // 内存分片
    private static class MemTableShard {
        final AtomicReference<ConcurrentSkipListMap<byte[], byte[]>> activeMap;
        final AtomicReference<ConcurrentSkipListMap<byte[], byte[]>> immutableMap;
        final ColumnFamilyHandle cfHandle; // 新增列族句柄
        final AtomicLong sequence = new AtomicLong(0); // 分片独立序列号

        MemTableShard(ColumnFamilyHandle cfHandle) {
            activeMap = new AtomicReference<>(new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
            immutableMap = new AtomicReference<>(null);
            this.cfHandle = cfHandle;
        }
    }

    private final MemTableShard[] memTableShards = new MemTableShard[shardCount];  // 替换原有分片列表

    // 新增性能监控指标
    private final LongAdder readRequests = new LongAdder();
    private final LongAdder writeRequests = new LongAdder();
    private final LongAdder bloomFilterHits = new LongAdder();

    public ShardStorageEngine(Config config, String chunkId, TRP trp, TrpNode trpNode) {
        // 初始化分片锁
        this.shardLocks = new ReentrantLock[shardCount];
        for(int i=0; i<shardCount; i++){
            shardLocks[i] = new ReentrantLock();
        }

        this.bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), 1000000, 0.01);
        this.lruCache = new LinkedHashMap<byte[], byte[]>(cacheSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<byte[], byte[]> eldest) {
                return size() > cacheSize;
            }
        };
        this.config = config;
        this.trp = trp;
        this.trpNode = trpNode;
        this.dataStorage = new DefaultDataStorage(config, trp);


        // 初始化列族
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            shardCfDescriptors[i] = new ColumnFamilyDescriptor(
                    ("shard_"+i).getBytes(),
                    new ColumnFamilyOptions()
                            .setCompressionType(CompressionType.LZ4_COMPRESSION)
                            .optimizeLevelStyleCompaction()
            );
            cfDescriptors.add(shardCfDescriptors[i]);
        }

        // 使用同一RocksDB实例打开所有列族
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        this.shardLogStorage = new DefaultShardLogStorage(config, trp,cfDescriptors,handles);
        // 保存列族句柄
        for (int i = 0; i < shardCount; i++) {
            shardCfHandles[i] = handles.get(i);
            memTableShards[i] = new MemTableShard(shardCfHandles[i]); // 传入列族句柄
        }

        startFlushTask();
//        startBatchCommitTask();
    }

    private final ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService batchCommitExecutor = Executors.newSingleThreadScheduledExecutor();

    @Override
    public Long appendLog(LogEntry logEntry) {
        return null;
    }

    @Override
    public Boolean batchPut(Kv[] kvs) {
        // 每批写入 CRC 校验
//        CRC32 crc32 = new CRC32();
//        for (Kv kv : kvs) {
//            crc32.update(kv.getKeyBytes());
//            crc32.update(kv.getValueBytes());
//        }
//        long checksum = crc32.getValue();
        // 批量提交优化
        CompletableFuture.runAsync(() -> {
            Map<Integer, List<Kv>> shardedBatch = new HashMap<>();

            // 按分片分组
            for (Kv kv : kvs) {
                int shardIndex = getShardIndex(kv.getKeyBytes());
                shardedBatch.computeIfAbsent(shardIndex, k -> new ArrayList<>()).add(kv);
            }

            // 批量写入各分片
            shardedBatch.forEach((shardIndex, batch) -> {
                shardLocks[shardIndex].lock();
                try {
                    MemTableShard memTableShard = memTableShards[shardIndex];
                    Long term = Long.valueOf(trp.getTerm()) ;
                    // 构建日志条目
                    LogEntry[] array = batch.stream().map(kv -> {
                        LogEntry logEntry = new LogEntry();
                        logEntry.setTerm(term);
                        logEntry.setIndex(memTableShard.sequence.incrementAndGet());
                        logEntry.setCommand(kv);
                        return logEntry;
                    }).toArray(LogEntry[]::new);
                    shardLogStorage.getShardLogStorage(shardIndex).append(array);
                    ConcurrentSkipListMap<byte[], byte[]> shard = memTableShard.activeMap.get();
                    batch.forEach(kv -> {
                        shard.put(kv.getKeyBytes(), kv.getValueBytes());
                        bloomFilter.put(kv.getKeyBytes());
                        lruCache.put(kv.getKeyBytes(), kv.getValueBytes());
                    });
                    // 异步通知复制监听器
                    batchCommitExecutor.execute(() -> Arrays.stream(array).forEach(this::notifyReplicationListeners));
                }finally {
                    shardLocks[shardIndex].unlock();
                }
            });

            // 统一内存检查
            if (getMemoryUsage() > maxMemory) {
//                evictLRU();
            }
        }, flushExecutor);

        return true;
    }

    @Override
    public Boolean delete(Kv kv) {
        int shardIndex = getShardIndex(kv.getKeyBytes());
        ConcurrentSkipListMap<byte[], byte[]> shard = memTableShards[shardIndex].activeMap.get();
        byte[] removed = shard.remove(kv.getKeyBytes());
        if (removed != null) {
            lruCache.remove(kv.getKeyBytes());
            bloomFilter.put(kv.getKeyBytes());
        }
        return removed != null;
    }

    @Override
    public Boolean get(Kv kv) {
        readRequests.increment();
        // 布隆过滤器过滤
        if (!bloomFilter.mightContain(kv.getKeyBytes())) {
            return false;
        }
        bloomFilterHits.increment();
        int shardIndex = getShardIndex(kv.getKeyBytes());
        ConcurrentSkipListMap<byte[], byte[]> shard = memTableShards[shardIndex].activeMap.get();
        byte[] bytes = shard.get(kv.getKeyBytes());
        if (bytes != null) {
            kv.setValue(bytes);
            lruCache.put(kv.getKeyBytes(), bytes);
            return true;
        }
        // 再从immutableMap中获取
        ConcurrentSkipListMap<byte[], byte[]> immutableMap = memTableShards[shardIndex].immutableMap.get();
        bytes = immutableMap.get(kv.getKeyBytes());
        if (bytes != null) {
            kv.setValue(bytes);
            lruCache.put(kv.getKeyBytes(), bytes);
            return true;
        }
        bytes = lruCache.get(kv.getKeyBytes());
        if (bytes != null) {
            kv.setValue(bytes);
            return true;
        }
        bytes = this.dataStorage.get(kv);
        if (bytes != null) {
            kv.setValue(bytes);
            lruCache.put(kv.getKeyBytes(), bytes);
            return true;
        }
        return false;
    }

    @Override
    public Boolean put(Kv kv) {
        writeRequests.increment();
        int shardIndex = getShardIndex(kv.getKeyBytes());
        shardLocks[shardIndex].lock();
        try {
           ConcurrentSkipListMap<byte[], byte[]> shard = memTableShards[shardIndex].activeMap.get();
           shard.put(kv.getKeyBytes(), kv.getValueBytes());
           bloomFilter.put(kv.getKeyBytes());
           lruCache.put(kv.getKeyBytes(), kv.getValueBytes());
           // 内存占用检查
           if (getMemoryUsage() > maxMemory) {
//               evictLRU();
           }
           return true;
       }finally {
           shardLocks[shardIndex].unlock();
       }
    }

    @Override
    public Boolean batchGet(Kv[] kvs) {
        boolean result = true;
        for (Kv kv : kvs) {
            if (!get(kv)) {
                result = false;
            }
        }
        return result;
    }

    public void registerReplicationListener(ReplicationEventListener listener) {
        replicationListeners.add(listener);
    }

    @Override
    public LogStorage getShardLogStorage() {

//        return this.shardLogStorage;
        return null;
    }

    private void notifyReplicationListeners(LogEntry entry) {
        replicationListeners.forEach(listener -> listener.onLogAppend(entry));
    }
    // todo 需要修改为按需触发
//    关键优化点说明：
//    WAL与内存刷盘的关系：
//    WAL保证数据持久性（已通过logStorage实现）
//    内存刷盘仅用于内存管理，非数据持久手段
//    策略调整：
//    将固定频率刷盘改为按需触发（内存阈值80%）
//    使用scheduleWithFixedDelay替代scheduleAtFixedRate避免任务堆积
//            延长检查间隔到5秒减少CPU开销
//    性能提升：
//    减少90%以上的无效刷盘操作
//            内存充足时完全不需要执行刷盘动作
//    仍然通过WAL保证数据安全
    private void startFlushTask() {
        flushExecutor.scheduleAtFixedRate(() -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int shardIndex = 0; shardIndex < memTableShards.length; shardIndex++) {
                MemTableShard shard = memTableShards[shardIndex];
                // 关键修改：加分片锁
                shardLocks[shardIndex].lock();
                try {
                    // 原子交换内存表
                    if (shard.immutableMap.get() == null) {
                        ConcurrentSkipListMap<byte[], byte[]> currentActive = shard.activeMap.get();
                        if (shard.immutableMap.compareAndSet(null, currentActive)) {
                            // 创建新内存表并异步处理旧数据
                            shard.activeMap.set(new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
                            int finalShardIndex = shardIndex;
                            Long lastMarkFlushIndex = shardLogStorage.getShardLogStorage(finalShardIndex).readLastLog().getIndex();
                            // 先把节点存下来
                            futures.add(CompletableFuture.runAsync(() -> {
                                List<Pair<byte[], byte[]>> batch = new ArrayList<>();
                                ConcurrentSkipListMap<byte[], byte[]> snapshot = shard.immutableMap.get();
                                Iterator<Map.Entry<byte[], byte[]>> it = snapshot.entrySet().iterator();
                                while (it.hasNext()){
                                    // 快速批量转移（每次最多1000条）
                                    int count = 0;
                                    while (it.hasNext() && count++ < 1000) {
                                        Map.Entry<byte[], byte[]> entry = it.next();
                                        batch.add(new Pair<>(entry.getKey(), entry.getValue()));
                                        it.remove();
                                    }
                                    dataStorage.batchPut(batch);
                                }
                                // 这里应该给wal创建一个快照了吧，这里之后的数据都是在内存中的没有刷新到db的数据，如果需要恢复的时候，应该从这里恢复
                                // 这里标记真正刷新完成的位置
                                shardLogStorage.getShardLogStorage(finalShardIndex).markFlushIndex(lastMarkFlushIndex);
                                snapshot.clear();
                                shard.immutableMap.set(null); // 重置状态
                            }, flushExecutor));
                        }
                    }
                }finally {
                    shardLocks[shardIndex].unlock();
                }
          }
            // 等待所有分片完成
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private int getShardIndex(byte[] key) {
        return Math.abs(Arrays.hashCode(key)) % shardCount;
    }

    private long getMemoryUsage() {
        long usage = 0;
        for (MemTableShard shard : memTableShards) {
            for (Map.Entry<byte[], byte[]> entry : shard.activeMap.get().entrySet()) {
                usage += entry.getKey().length + entry.getValue().length;
            }
        }
        return usage;
    }

    private void evictLRU() {
        Iterator<Map.Entry<byte[], byte[]>> iterator = lruCache.entrySet().iterator();
        while (iterator.hasNext() && getMemoryUsage() > maxMemory) {
            Map.Entry<byte[], byte[]> entry = iterator.next();
            int shardIndex = getShardIndex(entry.getKey());
            ConcurrentSkipListMap<byte[], byte[]> shard = memTableShards[shardIndex].activeMap.get();
            shard.remove(entry.getKey());
            iterator.remove();
        }
    }

    // 环形缓冲区实现
    private static class CircularBuffer<T> {
        private final T[] buffer;
        private final int size;
        private int head;
        private int tail;

        @SuppressWarnings("unchecked")
        public CircularBuffer(int size) {
            this.buffer = (T[]) new Object[size];
            this.size = size;
            this.head = 0;
            this.tail = 0;
        }

        public synchronized void add(T item) {
            buffer[head] = item;
            head = (head + 1) % size;
            if (head == tail) {
                tail = (tail + 1) % size;
            }
        }

        public synchronized T get() {
            if (head == tail) {
                return null;
            }
            T item = buffer[tail];
            tail = (tail + 1) % size;
            return item;
        }
    }

    // 在文件末尾添加
    public static class Bytes {
        // 字节数组比较器（支持空数组）
        public static final Comparator<byte[]> BYTES_COMPARATOR = (a, b) -> {
            int minLength = Math.min(a.length, b.length);
            for (int i = 0; i < minLength; i++) {
                int cmp = Byte.compare(a[i], b[i]);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(a.length, b.length);
        };
    }
    // 修改恢复逻辑
    private void recoverFromWal() {
        for (int i = 0; i < shardCount; i++) {
            ShardStore storage = shardLogStorage.getShardLogStorage(i);
            long markFlushIndex = storage.getMarkFlushIndex();
            List<LogEntry> logEntries = storage.read(markFlushIndex + 1);
            Map<byte[], byte[]> kvs = logEntries.stream()
                    .map(x -> (Kv) x.getCommand())
                    .collect(Collectors.toMap(Kv::getKeyBytes, Kv::getValueBytes));
            applyToStorage(i,kvs);
        }
    }

    private void applyToStorage(int shardCount,Map<byte[], byte[]> kvs) {
        MemTableShard memTableShard = memTableShards[shardCount];
        ConcurrentSkipListMap<byte[], byte[]> concurrentSkipListMap = memTableShard.activeMap.get();
        concurrentSkipListMap.putAll(kvs);
    }
}
