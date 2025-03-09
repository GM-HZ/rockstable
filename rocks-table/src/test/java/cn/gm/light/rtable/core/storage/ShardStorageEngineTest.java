package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.StorageEngine;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.storage.shard.ShardStorageEngine;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.TRP;
import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/9 08:51:58
 */
@Slf4j
public class ShardStorageEngineTest {
    StorageEngine engine;

    @Test
    public void testPut() {
        log.info("testPut");
        // 创建测试配置和对象
        Config config = new Config();
        config.setNodePort(8888);
        config.setEnableAsyncWal(false);
        config.setMemoryShardNum(1);
        TRP trp = TRP.builder().tableName("test_table")
                .replicationId(0)
                .partitionId(0)
                .isLeader(true)
                .term(0)
                .build();
        engine = new ShardStorageEngine(config, "chunkId", trp);
        Kv kv =  new Kv();
        kv.setFamily("f");
        kv.setKey("key1");
        kv.setColumn("c");
        kv.setValue("value1");
        // 调用 put 方法
        boolean result = engine.put(kv);
        Assertions.assertTrue(result);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Kv newkv =  new Kv();
        newkv.setFamily("f");
        newkv.setKey("key1");
        newkv.setColumn("c");
        Boolean b = engine.get(newkv);
        Assertions.assertTrue(b);
        log.info("{}", JSON.toJSONString(newkv));

    }

    @Test
    public void testPut1() {
        Kv newkv =  new Kv();
        newkv.setFamily("f");
        newkv.setKey("key1");
        newkv.setColumn("c");
        Boolean b = engine.get(newkv);
        Assertions.assertTrue(b);
        JSON.toJSONString(newkv);
    }

}
