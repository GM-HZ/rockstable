package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.LogStorage;
import cn.gm.light.rtable.core.StorageEngine;
import cn.gm.light.rtable.core.TrpNode;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.utils.Pair;
import com.alibaba.fastjson2.JSON;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;



@Slf4j
public class DefaultStorageEngine implements StorageEngine {
    private final List<ReplicationEventListener> replicationListeners = new CopyOnWriteArrayList<>();
    private final DefaultDataStorage dataStorage;
    private final DefaultLogStorage logStorage;
    private final TRP trp;
    private final TrpNode trpNode;
    private final Config config;
    // 布隆过滤器
    private final BloomFilter<byte[]> bloomFilter;
    // 环形缓冲区
    private final CircularBuffer<LogEntry> circularBuffer;
    private final int bufferSize = 64 * 1024;
    private final ReentrantLock bufferLock = new ReentrantLock();
    private final AtomicLong bufferIndex = new AtomicLong(0);
    // LRU 缓存
    private final LinkedHashMap<byte[], byte[]> lruCache;
    private final int cacheSize =  64 * 1024;
    private final int shardCount = Runtime.getRuntime().availableProcessors() * 2;
    private final int maxMemory = 256 * 1024 * 1024 * shardCount; // 预设内存阈值
    // 新增内存分片锁优化
    private ReentrantLock[] shardLocks;  // 每个分片独立锁

    // 替换原有环形缓冲区
    private final Disruptor<LogEntryEvent> disruptor;
    private final RingBuffer<LogEntryEvent> ringBuffer;
    private static final int BUFFER_SIZE = 1024 * 1024; // 调整为2的幂次方

    // Disruptor事件类
    private static class LogEntryEvent {
        LogEntry value;
        void set(LogEntry entry) { this.value = entry; }
    }

    // 内存分片
    private static class MemTableShard {
        final AtomicReference<ConcurrentSkipListMap<byte[], byte[]>> activeMap;
        final AtomicReference<ConcurrentSkipListMap<byte[], byte[]>> immutableMap;

        MemTableShard() {
            activeMap = new AtomicReference<>(new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
            immutableMap = new AtomicReference<>(null);
        }
    }

    private final MemTableShard[] memTableShards = new MemTableShard[shardCount];  // 替换原有分片列表


    // 新增性能监控指标
    private final LongAdder readRequests = new LongAdder();
    private final LongAdder writeRequests = new LongAdder();
    private final LongAdder bloomFilterHits = new LongAdder();

    public DefaultStorageEngine(Config config, String chunkId, TRP trp, TrpNode trpNode) {
        // 初始化分片锁
        this.shardLocks = new ReentrantLock[shardCount];
        for(int i=0; i<shardCount; i++){
            shardLocks[i] = new ReentrantLock();
        }
        for (int i = 0; i < shardCount; i++) {
            memTableShards[i] = new MemTableShard();  // 初始化新结构
        }
        this.bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), 1000000, 0.01);
        this.circularBuffer = new CircularBuffer<>(bufferSize);
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
        this.logStorage = new DefaultLogStorage(config, trp);
        // Disruptor初始化
        this.disruptor = new Disruptor<>(
                LogEntryEvent::new,
                BUFFER_SIZE,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,  // 支持多生产者
                new BlockingWaitStrategy()
        );

        // 设置消费者
        disruptor.handleEventsWith(this::batchCommitHandler);
        this.ringBuffer = disruptor.start();

        startFlushTask();
        startBatchCommitTask();
    }

    // 消费者处理逻辑
    private void batchCommitHandler(LogEntryEvent event, long sequence, boolean endOfBatch) {
        if (event.value != null) {
            Long append = this.logStorage.append(new LogEntry[]{event.value});
        }
    }
    private final ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService batchCommitExecutor = Executors.newSingleThreadScheduledExecutor();

    @Override
    public Long appendLog(LogEntry logEntry) {

        long sequence = ringBuffer.next();
        try {
            LogEntryEvent event = ringBuffer.get(sequence);
            event.set(logEntry);
        } finally {
            ringBuffer.publish(sequence);
        }
        return sequence; // 返回序列号作为日志ID
//       bufferLock.lock();
//        try {
//            circularBuffer.add(logEntry);
//            bufferIndex.incrementAndGet();
//            if (bufferIndex.get() % bufferSize == 0) {
//                batchCommitExecutor.submit(this::batchCommitLogs);
//            }
//        } finally {
//            bufferLock.unlock();
//        }
//        return this.logStorage.append(new LogEntry[]{logEntry});
    }

    private void batchCommitLogs() {
        List<LogEntry> batch = new ArrayList<>();
        bufferLock.lock();
        try {
            for (int i = 0; i < bufferSize; i++) {
                LogEntry entry = circularBuffer.get();
                if (entry != null) {
                    batch.add(entry);
                }
            }
            bufferIndex.set(0);
        } finally {
            bufferLock.unlock();
        }
        if (!batch.isEmpty()) {
            this.logStorage.append(batch.toArray(new LogEntry[0]));
        }
    }

    @Override
    public Boolean batchPut(Kv[] kvs) {
        // 每批写入 CRC 校验
        CRC32 crc32 = new CRC32();
        for (Kv kv : kvs) {
            crc32.update(kv.getKeyBytes());
            crc32.update(kv.getValueBytes());
        }
        long checksum = crc32.getValue();
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
                    ConcurrentSkipListMap<byte[], byte[]> shard = memTableShards[shardIndex].activeMap.get();
                    batch.forEach(kv -> {
                        shard.put(kv.getKeyBytes(), kv.getValueBytes());
                        bloomFilter.put(kv.getKeyBytes());
                        lruCache.put(kv.getKeyBytes(), kv.getValueBytes());
                    });
                }finally {
                    shardLocks[shardIndex].unlock();
                }
            });

            // 统一内存检查
            if (getMemoryUsage() > maxMemory) {
                evictLRU();
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
               evictLRU();
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
        return this.logStorage;
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
            for (MemTableShard shard : memTableShards) {
                // 原子交换内存表
                if (shard.immutableMap.get() == null) {
                    ConcurrentSkipListMap<byte[], byte[]> currentActive = shard.activeMap.get();
                    if (shard.immutableMap.compareAndSet(null, currentActive)) {
                        // 创建新内存表并异步处理旧数据
                        shard.activeMap.set(new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
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
                            snapshot.clear();
                            shard.immutableMap.set(null); // 重置状态
                        }, flushExecutor));
                    }
                }
          }
            // 等待所有分片完成
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }
    public Kv buildKv(byte[] key, byte[] value) {
        String jsonString = JSON.toJSONString(key);
        String[] split = jsonString.split("#");
        return Kv.builder().family(split[0]).key(split[1]).column(split[2]).value(value).build();
    }

    private void startBatchCommitTask() {
        batchCommitExecutor.scheduleAtFixedRate(this::batchCommitLogs, 100, 100, TimeUnit.MILLISECONDS);
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
}
