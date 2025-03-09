//package cn.gm.light.rtable.core.storage;
//
//import cn.gm.light.rtable.core.TrpNode;
//import cn.gm.light.rtable.core.config.Config;
//import cn.gm.light.rtable.entity.Kv;
//import cn.gm.light.rtable.entity.LogEntry;
//import cn.gm.light.rtable.entity.TRP;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//
//import java.util.concurrent.CompletableFuture;
//
//import static org.mockito.Mockito.mock;
//
//public class DefaultStorageEngineTest {
//
//    @Test
//    public void testAppendLog() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        LogEntry logEntry = new LogEntry();
//
//        // 调用 appendLog 方法
//        long sequence = engine.appendLog(logEntry);
//
//        // 验证结果
//        Assertions.assertNotNull(sequence);
//    }
//
//    @Test
//    public void testBatchPut() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        Kv kv1 =  Kv.builder().family("f").key("key1").column("c").value("value1").build();
//        Kv kv2 =  Kv.builder().family("f").key("key2").column("c").value("value2").build();
//
//
//        // 调用 batchPut 方法
//        boolean result = engine.batchPut(new Kv[]{kv1, kv2});
//
//        // 验证结果
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    public void testDelete() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        Kv kv =  Kv.builder().family("f").key("key1").column("c").value("value1").build();
//
//
//        // 调用 delete 方法
//        boolean result = engine.delete(kv);
//
//        // 验证结果
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    public void testGet() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        Kv kv =  Kv.builder().family("f").key("key1").column("c").value("value1").build();
//
//
//        // 调用 get 方法
//        boolean result = engine.get(kv);
//
//        // 验证结果
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    public void testPut() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        Kv kv =  Kv.builder().family("f").key("key1").column("c").value("value1").build();
//
//
//        // 调用 put 方法
//        boolean result = engine.put(kv);
//        kv.setValue(null);
//        Boolean b = engine.get(kv);
//        // 验证结果
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    public void testBatchGet() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        Kv kv1 =  Kv.builder().family("f").key("key1").column("c").value("value1").build();
//        Kv kv2 =  Kv.builder().family("f").key("key2").column("c").value("value2").build();
//
//        // 调用 batchGet 方法
//        boolean result = engine.batchGet(new Kv[]{kv1, kv2});
//
//        // 验证结果
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    public void testStartFlushTask() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        // 模拟定时任务执行
//        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
//            // 模拟数据处理
//        });
//
//        // 等待任务完成
//        future.join();
//    }
//
//    @Test
//    public void testStartBatchCommitTask() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        // 模拟定时任务执行
//        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
//            // 模拟数据提交
//        });
//
//        // 等待任务完成
//        future.join();
//    }
//
//    @Test
//    public void testGetShardIndex() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
//        byte[] key = "testKey".getBytes();
//
//        // 调用 getShardIndex 方法
////        int shardIndex = engine.getShardIndex(key);
////
////        // 验证结果
////        Assertions.assertTrue(shardIndex >= 0 && shardIndex < engine.shardCount);
//    }
//
//    @Test
//    public void testGetMemoryUsage() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
////        // 模拟内存使用情况
////        engine.memTableShards[0].activeMap.get().put("key1".getBytes(), "value1".getBytes());
////        engine.memTableShards[1].activeMap.get().put("key2".getBytes(), "value2".getBytes());
////
////        // 调用 getMemoryUsage 方法
////        long memoryUsage = engine.getMemoryUsage();
//
//        // 验证结果
////        Assertions.assertTrue(memoryUsage > 0);
//    }
//
//    @Test
//    public void testEvictLRU() {
//        // 创建测试配置和对象
//        Config config = mock(Config.class);
//        TRP trp = mock(TRP.class);
//        TrpNode trpNode = mock(TrpNode.class);
//        DefaultStorageEngine engine = new DefaultStorageEngine(config, "chunkId", trp, trpNode);
//
////        Kv kv1 = new Kv();
////        Kv kv2 = new Kv();
//
//        // 调用 put 方法添加数据
////        engine.put(kv1);
////        engine.put(kv2);
//
//        // 调用 evictLRU 方法
////        engine.evictLRU();
//
//        // 验证数据是否被移除
////        Assertions.assertFalse(engine.lruCache.containsKey(kv1.getKeyBytes()));
////        Assertions.assertFalse(engine.lruCache.containsKey(kv2.getKeyBytes()));
//    }
//}