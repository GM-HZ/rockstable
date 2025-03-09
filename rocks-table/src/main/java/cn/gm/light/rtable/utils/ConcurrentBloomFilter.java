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
    public void add(byte[] element) {
        long[] hashes = computeHashes(element);
        for (long hash : hashes) {
            // 修复点：确保bitIndex非负
            int bitIndex = (int) ((hash & Long.MAX_VALUE) % size); // 位运算消除符号位
            int segment = bitIndex % locks.length;
            long stamp = locks[segment].writeLock();
            try {
                // 二次校验防止负索引（防御性编程）
                if (bitIndex < 0) {
                    throw new IllegalStateException("Negative bit index detected: " + bitIndex);
                }
                int arrayIndex = bitIndex / 64;
                int bitInLong = bitIndex % 64;
                long mask = 1L << bitInLong;
                bits.getAndUpdate(arrayIndex, old -> old | mask);
            } finally {
                locks[segment].unlockWrite(stamp);
            }
        }
    }

    // 修改存在性检查方法（添加乐观锁）
    @Override
    public boolean mightContain(byte[] element) {
        long[] hashes = computeHashes(element);
        for (long hash : hashes) {
            int bitIndex = (int) ((hash & Long.MAX_VALUE) % size); // 确保非负
            int segment = bitIndex % locks.length;

            // 使用乐观读锁
            long stamp = locks[segment].tryOptimisticRead();
            int arrayIndex = bitIndex / 64;
            int bitInLong = bitIndex % 64;
            long mask = 1L << bitInLong;
            long value = bits.get(arrayIndex);

            // 验证期间没有写锁
            if (!locks[segment].validate(stamp)) {
                stamp = locks[segment].readLock();
                try {
                    value = bits.get(arrayIndex);
                } finally {
                    locks[segment].unlockRead(stamp);
                }
            }
            if ((value & mask) == 0) {
                return false;
            }
        }
        return true;
    }

    // 修改哈希计算逻辑
    private long[] computeHashes(byte[] element) {
        long[] hashes = new long[hashFunctions];
        long hash1 = murmurHash64(element, 0x9747b28c); // 使用固定种子
        long hash2 = murmurHash64(element, hash1);
        for (int i = 0; i < hashFunctions; i++) {
            hashes[i] = (hash1 + i * hash2) % size; // 移除绝对值保证正负分布
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


    // 实现完整MurmurHash（修复哈希碰撞问题）
    private long murmurHash64(byte[] data, long seed) {
        int length = data.length;
        long h = seed ^ (length * 0xc6a4a7935bd1e995L);
        int remaining = length;

        for (int i = 0; i + 8 <= length; i += 8) {
            long k = ((long) data[i] & 0xff)
                    | (((long) data[i+1] & 0xff) << 8)
                    | (((long) data[i+2] & 0xff) << 16)
                    | (((long) data[i+3] & 0xff) << 24)
                    | (((long) data[i+4] & 0xff) << 32)
                    | (((long) data[i+5] & 0xff) << 40)
                    | (((long) data[i+6] & 0xff) << 48)
                    | (((long) data[i+7] & 0xff) << 56);

            k *= 0xc6a4a7935bd1e995L;
            k ^= k >>> 47;
            k *= 0xc6a4a7935bd1e995L;
            h ^= k;
            h *= 0xc6a4a7935bd1e995L;
        }

        if (remaining > 0) {
            long k = 0;
            for (int i = length - remaining; i < length; i++) {
                k <<= 8;
                k |= (data[i] & 0xff);
            }
            h ^= k;
            h *= 0xc6a4a7935bd1e995L;
        }

        h ^= h >>> 47;
        h *= 0xc6a4a7935bd1e995L;
        h ^= h >>> 47;
        return h;
    }
}