package cn.gm.light.rtable.core.storage.shard;

import cn.gm.light.rtable.core.LogStorage;
import cn.gm.light.rtable.core.ShardLogStorage;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.storage.shard.ShardStore;
import cn.gm.light.rtable.core.storage.shard.ShardStoreFactory;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.utils.LongToByteArray;
import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 18:04:00
 */
@Slf4j
public class DefaultShardLogStorage {
    private TRP trp;
    private final String dataDir;
    private RocksDB logDB;
    private List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    private List<ColumnFamilyHandle> columnFamilyHandles;
    private ShardStoreFactory shardStoreFactory;
    public DefaultShardLogStorage(Config config, TRP trp,List<ColumnFamilyDescriptor> columnFamilyDescriptors, List<ColumnFamilyHandle> columnFamilyHandles) {
        this.trp = trp;
        this.dataDir = initializeLogDir(config);
        this.columnFamilyDescriptors = columnFamilyDescriptors;
        this.columnFamilyHandles = columnFamilyHandles;
        try {
            initRocksDB();
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
        Map<Integer, ColumnFamilyHandle> collect = columnFamilyHandles.stream().collect(Collectors.toMap(ColumnFamilyHandle::getID, columnFamilyHandle -> columnFamilyHandle));
        shardStoreFactory = new ShardStoreFactory(logDB,collect);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }
    private String initializeLogDir(Config config) {
        String serialize = trp.serialize();
        String dir = config.getDataDir() == null ? "./rtable/log/" + serialize : config.getDataDir();
        File file = new File(dir);
        if (!file.exists()) {
            boolean success = file.mkdirs();
            if (success) {
                log.warn("Created a new directory: " + dir);
            }
        }
        return dir;
    }
    private void initRocksDB() throws RocksDBException {
        RocksDB.loadLibrary();
        if (columnFamilyDescriptors != null && columnFamilyHandles != null) {
            // 将默认列族作为第一个列族
            List<ColumnFamilyDescriptor> allDescriptors = new ArrayList<>();
            List<ColumnFamilyHandle> allHandles = new ArrayList<>();

            // 添加默认列族
            allDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

            // 添加分片列族
            allDescriptors.addAll(columnFamilyDescriptors);

            // 获取现有列族列表
            List<byte[]> existingCFs = RocksDB.listColumnFamilies(new Options(), dataDir);

            // 确保所有列族都存在
            for (ColumnFamilyDescriptor descriptor : columnFamilyDescriptors) {
                String cfName = new String(descriptor.getName());
                if (!existingCFs.contains(descriptor.getName())) {
                    log.info("Creating new column family: {}", cfName);
                }
            }

            DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            logDB = RocksDB.open(options, dataDir, allDescriptors, allHandles);

            // 调整列族句柄映射关系，将默认列族映射到索引 0，然后去除掉默认列族
            columnFamilyHandles.clear();
            columnFamilyHandles.addAll(allHandles.subList(1, allHandles.size()));
        }else {
            Options options = new Options().setCreateIfMissing(true);
            logDB = RocksDB.open(options, dataDir);
        }
    }

    public ShardStore getShardLogStorage(int shardId) {
        return shardStoreFactory.getStore(shardId);
    }

    public void stop() {
        if (logDB != null) {
            try {
                logDB.close();
                log.info("RocksDB closed successfully.");
                logDB = null; // 防止重复关闭
            } catch (Exception e) {
                log.error("Failed to close RocksDB", e);
            }
        }
    }
}
