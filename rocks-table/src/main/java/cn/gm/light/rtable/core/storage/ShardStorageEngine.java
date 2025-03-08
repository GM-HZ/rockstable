package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.LogStorage;
import cn.gm.light.rtable.core.StorageEngine;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.storage.shard.ShardStore;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.utils.BloomFilter;
import cn.gm.light.rtable.utils.ConcurrentBloomFilter;
import cn.gm.light.rtable.utils.Pair;
import cn.gm.light.rtable.utils.RtThreadFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;


import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
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
    private final Config config;
    // 布隆过滤器
    private final BloomFilter bloomFilter;

    // 替换原有LRU缓存定义
    private final Cache<ByteBuffer, byte[]> lruCache;

    private final int cacheSize;
    private final int shardCount;
    private final int maxMemory; // 预设内存阈值

    // 新增内存分片锁优化
    private ReentrantLock[] shardLocks;  // 每个分片独立锁
    // 在类定义中添加列族管理
    private final ColumnFamilyHandle[] shardCfHandles;
    private final ColumnFamilyDescriptor[] shardCfDescriptors;
    // 内存分片
    private static class MemTableShard {
        final AtomicReference<ConcurrentSkipListMap<byte[], byte[]>> activeMap;
        final AtomicReference<ConcurrentSkipListMap<byte[], byte[]>> immutableMap;
        final ColumnFamilyHandle cfHandle; // 新增列族句柄
        final LongAdder memoryCounter = new LongAdder(); // 新增分片内存计数器

        MemTableShard(ColumnFamilyHandle cfHandle) {
            activeMap = new AtomicReference<>(new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
            immutableMap = new AtomicReference<>(null);
            this.cfHandle = cfHandle;
        }
    }

    private final MemTableShard[] memTableShards;  // 替换原有分片列表

    // 新增性能监控指标
    private final LongAdder readRequests = new LongAdder();
    private final LongAdder writeRequests = new LongAdder();
    private final LongAdder bloomFilterHits = new LongAdder();

    private final ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService asyncExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService businessExecutor;

    private final Disruptor<ShardBatchEvent> disruptor;

    public ShardStorageEngine(Config config, String chunkId, TRP trp) {
        this.config = config;
        this.trp = trp;
        this.dataStorage = new DefaultDataStorage(config, trp);

//        初始化分片
        this.shardCount = Objects.isNull(config.getMemoryShardNum()) ? Runtime.getRuntime().availableProcessors() * 2 : config.getMemoryShardNum();
        // 初始化分片锁
        this.shardLocks = new ReentrantLock[shardCount];
        for(int i=0; i<shardCount; i++){
            shardLocks[i] = new ReentrantLock();
        }
        this.maxMemory = 256 * 1024 * 1024 * shardCount;
        this.cacheSize =  1024 * 1024;
        this.bloomFilter = new ConcurrentBloomFilter(1024 * 1024 * 1024, 0.01);

        this.lruCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)          // 最大容量
                .expireAfterWrite(5, TimeUnit.MINUTES) // 写入后过期
                .executor(Executors.newWorkStealingPool())
                .recordStats()                // 开启统计信息（用于监控）
                .build();

        // 初始化wal分片
        this.shardCfHandles = new ColumnFamilyHandle[shardCount];
        this.shardCfDescriptors = new ColumnFamilyDescriptor[shardCount];
        // 初始化列族
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .optimizeLevelStyleCompaction();
            shardCfDescriptors[i] = new ColumnFamilyDescriptor(("shard_"+i).getBytes(), columnFamilyOptions);
            cfDescriptors.add(shardCfDescriptors[i]);
        }

        // 使用同一RocksDB实例打开所有列族
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        this.shardLogStorage = new DefaultShardLogStorage(config, trp,cfDescriptors,handles);
        // 初始化内存分片
        this.memTableShards = new MemTableShard[shardCount];
        for (int i = 0; i < shardCount; i++) {
            shardCfHandles[i] = handles.get(i);
            memTableShards[i] = new MemTableShard(shardCfHandles[i]); // 传入列族句柄
        }
        businessExecutor = new ThreadPoolExecutor(
                shardCount * 2,
                shardCount * 4,
                60L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(shardCount * 1000),
                RtThreadFactory.forThreadPool("businessExecutor"), (r, executor) -> {
                    log.error("线程池已满，任务被拒绝：{}", r.toString());
                });
        // 初始化 Disruptor
        disruptor = new Disruptor<>(ShardBatchEvent::new,
                1024 * 1024,
                RtThreadFactory.forThreadPool("Shard-Processor"),
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            try {
                processShardBatch(event.shardIndex, event.batch);
            } catch (Exception e) {
                log.error("分片处理失败", e);
            }
        });
        disruptor.start();
        startFlushTask();
    }

    @Override
    public Long appendLog(LogEntry logEntry) {
        return null;
    }

    @Override
    public Boolean batchPut(Kv[] kvs) {
        // 批量提交优化
        CompletableFuture.runAsync(() -> {
            writeRequests.increment();
            // 避免线程切换
            // 分片收集数据
            Map<Integer, List<Kv>> shardedBatch = new HashMap<>();
            for (Kv kv : kvs) {
                int shardIndex = getShardIndex(kv.getKeyBytes());
                shardedBatch.computeIfAbsent(shardIndex, k -> new ArrayList<>()).add(kv);
            }
            // 批量写入各分片
            // 无需外部锁
            shardedBatch.forEach((shardIndex, batch) -> {
                // 使用 Disruptor 发布事件
                disruptor.getRingBuffer().publishEvent((event, sequence) -> {
                    event.set(shardIndex, batch);
                });
            });
        }, businessExecutor);

        // 基于概率的主动内存检查
        if (ThreadLocalRandom.current().nextDouble() < 0.3) { // 30%概率触发检查
            flushExecutor.execute(() -> {
                if (getMemoryUsage() > 0.8 * maxMemory) {
                    evictLRU();
                    new FlushMemTables().run();
                }
            });
        }
        return true;
    }

    // 在 disruptor.handleEventsWith() 中的处理器
    private void processShardBatch(int shardIndex, List<Kv> batch) {
        LogEntry[] array = getLogEntries(batch);
        // 0. 写入wal,内部封装了RocksDB，已经加锁了，暂时是同步的，可以增加异步逻辑
        shardLogStorage.getShardLogStorage(shardIndex).append(array);

        MemTableShard shard = memTableShards[shardIndex];
        // 1. 批量更新BloomFilter
        Set<byte[]> keys = batch.stream()
                .map(Kv::getKeyBytes)
                .collect(Collectors.toSet());
        bloomFilter.addAll(keys);

        // 2. 批量写入内存表
        // ConcurrentSkipListMap.put 是线程安全的
        Map<byte[], byte[]> kvMap = batch.stream()
                .collect(Collectors.toMap(
                        Kv::getKeyBytes,
                        Kv::getValueBytes,
                        (oldVal, newVal) -> newVal
                ));
        shard.activeMap.get().putAll(kvMap);

        // 3. 批量更新LRU缓存,lruCache 是线程安全的
        Map<ByteBuffer, byte[]> cacheEntries = batch.stream()
                .collect(Collectors.toMap(
                        kv -> ByteBuffer.wrap(kv.getKeyBytes()),
                        Kv::getValueBytes
                ));
        lruCache.putAll(cacheEntries);

        // 异步操作通知
        asyncExecutor.execute(() -> {
            // 新增内存统计（在锁内执行）
            long delta = batch.stream()
                    .mapToLong(kv ->
                            kv.getKeyBytes().length +
                                    (kv.getValueBytes() != null ? kv.getValueBytes().length : 0))
                    .sum();
            shard.memoryCounter.add(delta);
            Arrays.stream(array).forEach(this::notifyReplicationListeners);
        });
    }


    private LogEntry[] getLogEntries(List<Kv> batch) {
       final Long term = (long) trp.getTerm();
        // 构建日志条目
        LogEntry[] array = batch.stream().map(kv -> {
            LogEntry logEntry = new LogEntry();
            logEntry.setTerm(term);
            logEntry.setCommand(kv);
            return logEntry;
        }).toArray(LogEntry[]::new);
        return array;
    }

    @Override
    public Boolean delete(Kv kv) {
        int shardIndex = getShardIndex(kv.getKeyBytes());
        ConcurrentSkipListMap<byte[], byte[]> shard = memTableShards[shardIndex].activeMap.get();
        byte[] removed = shard.remove(kv.getKeyBytes());
        if (removed != null) {
            // 更新内存计数器
            MemTableShard memShard = memTableShards[shardIndex];
            memShard.memoryCounter.add(-(removed.length + kv.getKeyBytes().length));
            lruCache.invalidate(ByteBuffer.wrap(kv.getKeyBytes()));
            bloomFilter.add(kv.getKeyBytes());
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
        ByteBuffer keyBuf = ByteBuffer.wrap(kv.getKeyBytes());
        byte[] bytes = shard.get(kv.getKeyBytes());
        if (bytes != null) {
            kv.setValue(bytes);
            lruCache.put(keyBuf, bytes);
            return true;
        }
        // 再从immutableMap中获取
        ConcurrentSkipListMap<byte[], byte[]> immutableMap = memTableShards[shardIndex].immutableMap.get();
        bytes = immutableMap.get(kv.getKeyBytes());
        if (bytes != null) {
            kv.setValue(bytes);
            lruCache.put(keyBuf, bytes);
            return true;
        }
        bytes = lruCache.getIfPresent(keyBuf);
        if (bytes != null) {
            kv.setValue(bytes);
            return true;
        }
        bytes = this.dataStorage.get(kv);
        if (bytes != null) {
            kv.setValue(bytes);
            lruCache.put(keyBuf, bytes);
            return true;
        }
        return false;
    }

    @Override
    public Boolean put(Kv kv) {
        return this.batchPut(new Kv[]{kv});
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
        flushExecutor.scheduleAtFixedRate(new FlushMemTables(), 1000, 1000, TimeUnit.MILLISECONDS);
    }


    class FlushMemTables implements Runnable {
        @Override
        public void run() {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int shardIndex = 0; shardIndex < memTableShards.length; shardIndex++) {
                MemTableShard shard = memTableShards[shardIndex];
                int finalShardIndex = shardIndex;
                Long lastMarkFlushIndex = null;
                // 关键修改：加分片锁，缩小加锁范围
                shardLocks[shardIndex].lock();
                try {
                    // 原子交换内存表
                    if (shard.immutableMap.get() == null) {
                        ConcurrentSkipListMap<byte[], byte[]> currentActive = shard.activeMap.get();
                        if (shard.immutableMap.compareAndSet(null, currentActive)) {
                            // 创建新内存表并异步处理旧数据
                            shard.activeMap.set(new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
                            lastMarkFlushIndex = shardLogStorage.getShardLogStorage(finalShardIndex).readLastLog().getIndex();
                        }
                    }
                }finally {
                    shardLocks[shardIndex].unlock();
                }
                Long finalLastMarkFlushIndex = lastMarkFlushIndex;
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
                    shardLogStorage.getShardLogStorage(finalShardIndex).markFlushIndex(finalLastMarkFlushIndex);
                    snapshot.clear();
                    shard.immutableMap.set(null); // 重置状态
                }, flushExecutor));
            }
            // 等待所有分片完成
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }
    }

    private int getShardIndex(byte[] key) {
        return Math.abs(Arrays.hashCode(key)) % shardCount;
    }

    private long getMemoryUsage() {
        long total = 0;
        for (MemTableShard shard : memTableShards) {
            total += shard.memoryCounter.sum(); // O(1)时间获取分片内存量
        }
        return total;
    }

    private void evictLRU() {
        long currentUsage = getMemoryUsage(); // 初始计算
        Iterator<Map.Entry<ByteBuffer, byte[]>> iterator = lruCache.asMap().entrySet().iterator();

        while (iterator.hasNext() && currentUsage > maxMemory) {
            Map.Entry<ByteBuffer, byte[]> entry = iterator.next();
            // 累减内存占用
            ByteBuffer key = entry.getKey();
            currentUsage -= (key.array().length + entry.getValue().length);

            int shardIndex = getShardIndex(key.array());
            ConcurrentSkipListMap<byte[], byte[]> shard = memTableShards[shardIndex].activeMap.get();
            shard.remove(key.array());
            iterator.remove();
        }

        // 最终同步实际值（可选）
        if (currentUsage <= maxMemory) {
            getMemoryUsage();
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
            int batchSize = 1000;
            for (int j = 0; j < logEntries.size(); j += batchSize) {
                List<LogEntry> batch = logEntries.subList(i, Math.min(i + batchSize, logEntries.size()));
                Map<byte[], byte[]> kvs = batch.stream()
                        .map(x -> (Kv) x.getCommand())
                        .collect(Collectors.toMap(Kv::getKeyBytes, Kv::getValueBytes));
                applyToStorage(i, kvs);
            }
        }
    }

    private void applyToStorage(int shardCount,Map<byte[], byte[]> kvs) {
        MemTableShard memTableShard = memTableShards[shardCount];
        ConcurrentSkipListMap<byte[], byte[]> concurrentSkipListMap = memTableShard.activeMap.get();
        concurrentSkipListMap.putAll(kvs);
    }

    // 定义事件类型（分片和批次）
    public static class ShardBatchEvent {
        private int shardIndex;
        private List<Kv> batch;

        public void set(int shardIndex, List<Kv> batch) {
            this.shardIndex = shardIndex;
            this.batch = batch;
        }
    }
    // 在关闭存储引擎时
    public void shutdown() {
        disruptor.shutdown();
        // 关闭其他资源...
    }

}
