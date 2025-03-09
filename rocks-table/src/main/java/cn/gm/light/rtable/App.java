package cn.gm.light.rtable;

import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.storage.shard.ShardStorageEngine;
import cn.gm.light.rtable.entity.TRP;
import com.lmax.disruptor.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/9 21:49:17
 */
@Slf4j
public class App {
    public static void main(String[] args) {
        Config config = new Config();
        config.setMemoryShardNum(16);
//        config.setDataDir("./data");
        TRP trp = TRP.builder().tableName("test_table")
                .replicationId(0)
                .partitionId(0)
                .isLeader(true)
                .term(0)
                .build();
        ShardStorageEngine storageEngine = new ShardStorageEngine(config, "chunk_test_table_0_0", trp);
        try {
            storageEngine.shutdown();
        } catch (TimeoutException e) {
            log.error("shutdown error", e);
        }
        log.info("shutdown success");
        config.setMemoryShardNum(32);
        storageEngine = new ShardStorageEngine(config, "chunk_test_table_0_0", trp);
        try {
            storageEngine.shutdown();
        } catch (TimeoutException e) {
            log.error("shutdown error", e);
        }
        log.info("shutdown success2");
    }
}
