package cn.gm.light.rtable.core.storage.shard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// 新增分片工厂
public class ShardStoreFactory {
    private final Map<Integer, ShardStore> stores = new HashMap<>();
    public final Map<Integer, ColumnFamilyHandle> columnFamilyHandles;
    private final RocksDB logDB;
    public ShardStoreFactory(RocksDB logDB ,Map<Integer, ColumnFamilyHandle> columnFamilyHandles) {
        this.logDB = logDB;
        this.columnFamilyHandles = columnFamilyHandles;
    }
    public ShardStore newInstance(int shardId) {
        return stores.computeIfAbsent(shardId, id -> {
            // 第一列是默认列族，所以需要+1
            ColumnFamilyHandle cfHandle = columnFamilyHandles.get(id+1);
            return new ShardStoreImpl(logDB,cfHandle);
        });
    }
    public void close() {
        stores.clear();
    }
}