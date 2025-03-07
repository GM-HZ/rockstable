package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.DataStorage;
import cn.gm.light.rtable.core.LifeCycle;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.utils.LongToByteArray;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 18:04:00
 */
@Slf4j
public class DefaultDataStorage implements DataStorage, LifeCycle {

    private TRP trp;
    private final String dataDir;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong lastIndex = new AtomicLong(0);
    private RocksDB logDB;

    public DefaultDataStorage(Config config, TRP trp) {
        this.trp = trp;
        this.dataDir = initializeLogDir(config);
        try {
            initRocksDB();
            initLastIndex();
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private String initializeLogDir(Config config) {
        String serialize = trp.serialize();
        String dir = config.getDataDir() == null ? "./rtable/data/" + serialize : config.getDataDir();
        File file = new File(dir);
        if (!file.exists()) {
            boolean success = file.mkdirs();
            if (success) {
                log.warn("Created a new directory: " + dir);
            }
        }
        return dir;
    }

    private void initLastIndex() throws RocksDBException {
        try (RocksIterator it = logDB.newIterator()) {
            it.seekToLast();
            if (it.isValid()) {
                long index = LongToByteArray.bytesToLong(it.key());
                lastIndex.set(index);
            } else {
                // 确保初始状态正确
                lastIndex.set(0L);
            }
        }
    }

    private void initRocksDB() throws RocksDBException {
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        logDB = RocksDB.open(options, dataDir);
        initLastIndex();
    }


    @Override
    public void put(Kv kv) {
        this.lock.lock();
        try {
            byte[] key = kv.getKeyBytes();
            byte[] value = kv.getValueBytes();
            logDB.put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public byte[] get(Kv kv) {
        this.lock.lock();
        try {
            return logDB.get(kv.getKeyBytes());
        }catch (RocksDBException e) {
            throw new RuntimeException(e);
        }finally {
            this.lock.unlock();
        }
    }

    @Override
    public void delete(Kv kv) {

    }

    @Override
    public void batchPut(Kv[] kv) {

    }

    @Override
    public void batchGet(Kv[] kv) {

    }

    @Override
    public void batchDelete(Kv[] key) {

    }

    @Override
    public void init() {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
