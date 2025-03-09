package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.StorageEngine;
import cn.gm.light.rtable.core.TrpNode;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.TRP;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/9 08:51:58
 */
public class ShardStorageEngineTest {

    @Test
    public void testPut() {
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
        StorageEngine engine = new ShardStorageEngine(config, "chunkId", trp);

        Kv kv =  Kv.builder().family("f").key("key1").column("c").value("value1").build();


        // 调用 put 方法
        boolean result = engine.put(kv);
        Kv newkv =  Kv.builder().family("f").key("key1").column("c").build();
        Boolean b = engine.get(newkv);
        // 验证结果
        Assertions.assertTrue(result);
    }

}
