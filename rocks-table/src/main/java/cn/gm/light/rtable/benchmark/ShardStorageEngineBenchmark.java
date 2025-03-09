package cn.gm.light.rtable.benchmark;

import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.storage.ShardStorageEngine;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.TRP;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)          // 测试吞吐量（ops/ms）
@OutputTimeUnit(TimeUnit.MILLISECONDS)   // 输出时间单位
@Warmup(iterations = 3, time = 5)       // 预热3轮，每轮5秒
@Measurement(iterations = 5, time = 10) // 正式测试5轮，每轮10秒
@Threads(64)                            // 64线程模拟高并发
@Fork(1)                                // 单进程测试
@State(Scope.Benchmark)
public class ShardStorageEngineBenchmark {

    // 分片数配置（参数化测试）
    @Param({"32", "64", "128"})
    private int shardCount;

    // 存储引擎实例（线程共享）
    private ShardStorageEngine storageEngine;

    // 预生成测试数据（colfamily#key#colname格式）
    private String[][] testKeys;

    @Setup(Level.Trial)
    public void setup() {
        // 初始化存储引擎（模拟配置）
        Config config = new Config();
        config.setMemoryShardNum(shardCount);
        TRP trp = TRP.builder().tableName("test_table")
                .replicationId(0)
                .partitionId(0)
                .isLeader(true)
                .term(0)
                .build();
        storageEngine = new ShardStorageEngine(config, "chunk_1", trp);

        // 生成测试键（10万条数据，覆盖不同分片）
        int dataSize = 100_000;
        testKeys = new String[dataSize][];
        for (int i = 0; i < dataSize; i++) {
            String colFamily = "cf" + (i % 10);          // 10个列族
            String key = "key" + (i % 10000);            // 1万个唯一key
            String colName = "col" + (i % 50);           // 50个列
            testKeys[i] = new String[]{colFamily, key, colName};
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        storageEngine.shutdown(); // 关闭存储引擎
    }

    // 随机获取一个测试键（模拟真实分布）
    private String[] getRandomTestKey() {
        int index = ThreadLocalRandom.current().nextInt(testKeys.length);
        return testKeys[index];
    }

    // 构建完整键（colfamily#key#colname）
    private byte[] buildFullKey(String[] parts) {
        String fullKey = String.join("#", parts);
        return fullKey.getBytes();
    }

    @Benchmark
    public void writeThroughput(Blackhole blackhole) {
        // 构造写入数据（单次写入10条，模拟批量）
        Kv[] batch = new Kv[10];
        for (int i = 0; i < 10; i++) {
            String[] keyParts = getRandomTestKey();
            byte[] keyBytes = buildFullKey(keyParts);
            byte[] valueBytes = ("value_" + System.currentTimeMillis()).getBytes();
            batch[i] = Kv.builder().family(keyParts[0]).key(keyParts[1]).column(keyParts[2]).value(valueBytes).build();
        }
        storageEngine.batchPut(batch);
        blackhole.consume(batch); // 避免JIT优化忽略结果
    }

    @Benchmark
    public void readHitCache(Blackhole blackhole) {
        // 构造读取请求（命中缓存）
        String[] keyParts = getRandomTestKey();
        byte[] keyBytes = buildFullKey(keyParts);
        Kv kv = Kv.builder().family(keyParts[0]).key(keyParts[1]).column(keyParts[2]).value(null).build();
        boolean exists = storageEngine.get(kv);
        blackhole.consume(exists);
    }
    @Benchmark
    public void readMissCache(Blackhole blackhole) {
        // 构造不存在于缓存的Key（确保穿透到磁盘）
        String[] keyParts = new String[]{
                "cf_miss",
                "key_miss_" + ThreadLocalRandom.current().nextInt(),
                "col_miss"
        };
        byte[] keyBytes = buildFullKey(keyParts);
        Kv kv = Kv.builder().family(keyParts[0]).key(keyParts[1]).column(keyParts[2]).value(null).build();;
        boolean exists = storageEngine.get(kv);
        blackhole.consume(exists);
    }
}