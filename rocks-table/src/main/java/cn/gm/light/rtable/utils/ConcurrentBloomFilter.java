package cn.gm.light.rtable.utils;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.StampedLock;

public class ConcurrentBloomFilter implements BloomFilter{

    private final int size;                 // 位数组总长度（单位：bit）
    private final int hashFunctions;        // 哈希函数数量
    private final AtomicLongArray bits;     // 原子长整型数组（每个元素管理64位）
    private final StampedLock[] locks;      // 分段锁（减少锁竞争）

    // 构造方法（根据预期元素数量和误判率动态计算参数）
    public ConcurrentBloomFilter(long expectedElements, double falsePositiveRate) {
        this.size = calculateSize(expectedElements, falsePositiveRate);
        this.hashFunctions = calculateHashFunctions(expectedElements, size);
        this.bits = new AtomicLongArray((size + 63) / 64);  // 转换为长整型数组长度
        this.locks = new StampedLock[64];                   // 分段锁数量（按需调整）
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new StampedLock();
        }
    }

    @Override
    // 批量添加元素（线程安全）
    public void addAll(Iterable<byte[]> elements) {
        for (byte[] element : elements) {
            add(element);
        }
    }

    @Override
    // 添加单个元素（线程安全）
    public void add(byte[] element) {
        long[] hashes = computeHashes(element);
        for (long hash : hashes) {
            int bitIndex = (int) (hash % size);
            int segment = bitIndex % locks.length;  // 分段锁选择
            long stamp = locks[segment].writeLock();
            try {
                int arrayIndex = bitIndex / 64;
                int bitInLong = bitIndex % 64;
                long mask = 1L << bitInLong;
                bits.getAndUpdate(arrayIndex, old -> old | mask);
            } finally {
                locks[segment].unlockWrite(stamp);
            }
        }
    }

    @Override
    // 检查元素是否存在（线程安全）
    public boolean mightContain(byte[] element) {
        long[] hashes = computeHashes(element);
        for (long hash : hashes) {
            int bitIndex = (int) (hash % size);
            int arrayIndex = bitIndex / 64;
            int bitInLong = bitIndex % 64;
            long mask = 1L << bitInLong;
            if ((bits.get(arrayIndex) & mask) == 0) {
                return false;
            }
        }
        return true;
    }

    // 计算哈希值（模拟多个哈希函数）
    private long[] computeHashes(byte[] element) {
        long[] hashes = new long[hashFunctions];
        long hash1 = murmurHash64(element, 0);
        long hash2 = murmurHash64(element, hash1);
        for (int i = 0; i < hashFunctions; i++) {
            hashes[i] = Math.abs(hash1 + i * hash2) % size;
        }
        return hashes;
    }

    // 动态计算位数组大小
    private int calculateSize(long n, double p) {
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    // 动态计算哈希函数数量
    private int calculateHashFunctions(long n, int m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    // 简化版 MurmurHash 实现（实际应用需完整实现）
    private long murmurHash64(byte[] data, long seed) {
        // 此处应替换为完整 MurmurHash 实现（示例省略）
        return Math.abs(seed ^ data.hashCode());
    }
}