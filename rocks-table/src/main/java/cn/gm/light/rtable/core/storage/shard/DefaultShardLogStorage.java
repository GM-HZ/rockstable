package cn.gm.light.rtable.core.storage.shard;

import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.entity.TRP;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.util.*;
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
        Map<Integer, ColumnFamilyHandle> collect = columnFamilyHandles.stream().collect(Collectors.toMap(ColumnFamilyHandle::getID, columnFamilyHandle -> columnFamilyHandle, (x, y) -> y));
        shardStoreFactory = new ShardStoreFactory(logDB,collect);

        // 日志输出验证
        List<String> list = new ArrayList<>();
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            byte[] name = new byte[0];
            try {
                name = columnFamilyHandle.getName();
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            list.add(new String(name));
        }
        log.debug("Initialized {} column families: {}", columnFamilyHandles.size(),
                list);

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

            // 获取现有列族列表
            List<byte[]> existingCFs = RocksDB.listColumnFamilies(new Options(), dataDir);
            // 转换现有列族为Set方便比对（使用内容对比）
            Set<String> existingCFNames = existingCFs.stream()
                    .map(String::new)
                    .collect(Collectors.toSet());
            // 将默认列族作为第一个列族
            List<ColumnFamilyDescriptor> allDescriptors = new ArrayList<>();
            List<ColumnFamilyHandle> allHandles = new ArrayList<>();

            // 添加默认列族
            allDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

            // 合并逻辑优化
            Set<String> mergedCFs = new HashSet<>();
            // 添加历史分片列族
            // 1. 添加历史分片列族（排除默认）
            existingCFs.stream()
                    .filter(cf -> !Arrays.equals(cf, RocksDB.DEFAULT_COLUMN_FAMILY))
                    .forEach(cf -> {
                        String cfName = new String(cf);
                        if (mergedCFs.add(cfName)) {
                            allDescriptors.add(new ColumnFamilyDescriptor(cf));
                        }
                    });

            // 2. 添加新分片列族（带去重）
            for (ColumnFamilyDescriptor descriptor : columnFamilyDescriptors) {
                String cfName = new String(descriptor.getName());
                if (mergedCFs.add(cfName)) {
                    if (!existingCFNames.contains(cfName)) {
                        log.debug("Creating new column family: {}", cfName);
                    }
                    allDescriptors.add(descriptor);
                }
            }

            DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
            // 增加文件打开线程数
            options.setMaxFileOpeningThreads(128);
            // 增加最大后台作业线程数
            options.setMaxBackgroundJobs(16);
            // 调整WAL文件大小
            options.setMaxTotalWalSize(512 * 1024 * 1024L);
//            allDescriptors 必须包含历史所有的分片
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
        return shardStoreFactory.newInstance(shardId);
    }

    public void stop() {
        if (logDB != null) {
            try {
                // 1. 先同步所有写入
                logDB.syncWal(); // 强制同步WAL日志
                logDB.flush(new FlushOptions().setWaitForFlush(true)); // 强制刷新所有MemTable

                // 2. 关闭列族句柄（关键补充）
                shardStoreFactory.close();

                // 3. 关闭数据库实例（原有代码增强）
                try {
                    // 显式关闭（替代 try-with-resources）
                    logDB.close();
                }catch (Exception e) {
                    log.error("Failed to close RocksDB", e);
                } finally {
                    logDB = null;
                    log.info("RocksDB closed successfully.");
                }

                log.info("RocksDB closed successfully.");
            } catch (Exception e) {
                log.error("Failed to close RocksDB", e);
            }
        }
    }
}
