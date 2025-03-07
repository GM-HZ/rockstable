package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.core.LogStorage;
import cn.gm.light.rtable.core.StorageEngine;
import cn.gm.light.rtable.core.TrpNode;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.entity.TRP;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DefaultStorageEngine implements StorageEngine {
    private final List<ReplicationEventListener> replicationListeners = new CopyOnWriteArrayList<>();
    private final DefaultDataStorage dataStorage;
    private final DefaultLogStorage logStorage;
    private final TRP trp;
    private final TrpNode trpNode;
    private final Config config;
    private final ConcurrentSkipListMap<byte[], byte[]> memTable;


    public DefaultStorageEngine(Config config, String chunkId, TRP trp, TrpNode trpNode) {
        this.memTable = new ConcurrentSkipListMap<>();
        this.config = config;
        this.trp = trp;
        this.trpNode = trpNode;
        this.dataStorage = new DefaultDataStorage(config, trp);
        this.logStorage = new DefaultLogStorage(config, trp);
        startFlushTask();
    }


    private final ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();

    @Override
    public Long appendLog(LogEntry logEntry) {
        return this.logStorage.append(new LogEntry[]{logEntry});
    }

    @Override
    public Boolean batchPut(Kv[] kvs) {
        return null;
    }

    @Override
    public Boolean delete(Kv kv) {
        return null;
    }

    @Override
    public Boolean get(Kv kv) {
        byte[] bytes = memTable.get(kv.getKeyBytes());
        if (bytes != null) {
            kv.setValue(bytes);
            return Boolean.TRUE;
        }else{
            bytes = this.dataStorage.get(kv);
            if (bytes != null) {
                kv.setValue(bytes);
                return Boolean.TRUE;
            }
        }
        return false;
    }

    @Override
    public Boolean put(Kv kv) {
        byte[] put = memTable.put(kv.getKeyBytes(), kv.getValueBytes());
        return Boolean.TRUE;
    }

    @Override
    public Boolean batchGet(Kv[] kvs) {
        return null;
    }

    public void registerReplicationListener(ReplicationEventListener listener) {
        replicationListeners.add(listener);
    }

    @Override
    public LogStorage getLogStorage() {
        return this.logStorage;
    }

    private void notifyReplicationListeners(LogEntry entry) {
        replicationListeners.forEach(listener -> listener.onLogAppend(entry));
    }


    private void startFlushTask() {
        flushExecutor.scheduleAtFixedRate(() -> {
            synchronized (memTable) {
                try  {
                    List<Kv> batch = new ArrayList<>();
                    memTable.forEach((k, v) -> {
                        batch.add(Kv.builder().build());
                    });
                    this.dataStorage.batchPut(batch.toArray(new Kv[0]));
                    memTable.clear();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }, 50, 50, TimeUnit.MILLISECONDS);
    }

}