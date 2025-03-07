package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.DataStorage;
import cn.gm.light.rtable.core.LifeCycle;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.utils.LongToByteArray;
import cn.gm.light.rtable.utils.Pair;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
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
    public boolean put(Kv kv) {
        this.lock.lock();
        try {
            byte[] key = kv.getKeyBytes();
            byte[] value = kv.getValueBytes();
            logDB.put(key, value);
            return true;
        } catch (RocksDBException e) {
            return false;
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
    public byte[] get(byte[] k) {
        this.lock.lock();
        try {
            return logDB.get(k);
        }catch (RocksDBException e) {
            throw new RuntimeException(e);
        }finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean put(Pair<byte[], byte[]> kv) {
        this.lock.lock();
        try {
            logDB.put(kv.getKey(), kv.getValue());
            return true;
        }catch (RocksDBException e) {
            log.error("Failed to put key-value pair", e);
            return false;
        }finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean delete(byte[] k) {
        return false;
    }

    @Override
    public boolean batchPut(List<Pair<byte[], byte[]>> kvs) {
        this.lock.lock();
        try(WriteBatch batch = new WriteBatch()) {
            // 填充批量数据
            for (Pair<byte[], byte[]> entry : kvs) {
                batch.put(entry.getKey(), entry.getValue());
            }
            // 原子提交
            logDB.write(new org.rocksdb.WriteOptions(), batch);
            return true;
        } catch (RocksDBException e) {
            log.error("Failed to put key-value pair", e);
            return false;
        } finally {
            this.lock.unlock();
        }

    }

    @Override
    public List<Pair<byte[], byte[]>> batchGet(List<byte[]> keys) {
        this.lock.lock();
        try {
            List<Pair<byte[], byte[]>> results = new ArrayList<>(keys.size());
            for (byte[] key : keys) {
                byte[] value = logDB.get(key);
                results.add(new Pair<>(key, value));
            }
            return results;
        } catch (RocksDBException e) {
            throw new RuntimeException("Batch get failed", e);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean batchDelete(List<byte[]> k) {
        return false;
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
