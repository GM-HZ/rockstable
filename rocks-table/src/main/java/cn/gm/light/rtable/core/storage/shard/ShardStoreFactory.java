package cn.gm.light.rtable.core.storage.shard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// 新增分片工厂
public class ShardStoreFactory {
    private final Map<Integer, ShardStore> stores = new ConcurrentHashMap<>();
    public Map<Integer, ColumnFamilyHandle> columnFamilyHandles;
    private RocksDB logDB;
    public ShardStoreFactory(RocksDB logDB ,Map<Integer, ColumnFamilyHandle> columnFamilyHandles) {
        this.logDB = logDB;
        this.columnFamilyHandles = columnFamilyHandles;
    }
    public ShardStore getStore(int shardId) {
        return stores.computeIfAbsent(shardId, id -> {
            ColumnFamilyHandle cfHandle = columnFamilyHandles.get(id);
            return new ShardStoreImpl(logDB,cfHandle);
        });
    }
}