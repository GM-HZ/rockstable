package cn.gm.light.rtable.core;

import cn.gm.light.rtable.Chunk;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.queue.QueueCustomer;
import cn.gm.light.rtable.core.queue.RocketMQConsumer;
import cn.gm.light.rtable.core.replication.ReplicationsTask;
import cn.gm.light.rtable.core.storage.DefaultDataStorage;
import cn.gm.light.rtable.core.storage.DefaultLogStorage;
import cn.gm.light.rtable.entity.*;
import cn.gm.light.rtable.entity.dto.ProxyToNodeRequest;
import cn.gm.light.rtable.utils.TimerTask;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @project JavaStudy
 * @author 明溪
 * @version 1.0
 */
public class ChunkImpl implements Chunk {
    private String chunkId;
    private TRP trp;
    private Endpoint endpoint;
    private List<Endpoint> groups;
    private DataStorage dataStorage;
    private LogStorage logStorage;
    private Map<String, String> matchIndex;
    private Config config;
    private boolean isLeader;
    private long term;
    private Map<String, Long> commitIndex;
    private Map<String, String> lastApplied;
    private List<TimerTask> replicationTasks;
    private TrpNode trpNode;
    private QueueCustomer queueConsumer;

    public ChunkImpl(Config config,TRP trp,TrpNode trpNode) {
        this.trp = trp;
        this.config = config;
        this.chunkId = trp.serialize()+"#"+ UUID.randomUUID().toString().replace("-","");
        this.trpNode = trpNode;
    }


    @Override
    public void handleClientRequest(RequestCommand requestCommand, CompletableFuture<ResponseCommand> responseFuture){
        LogEntry logEntry = new LogEntry();
        logEntry.setCommand(requestCommand);
        logEntry.setTerm(term);
        Long append = this.logStorage.append(new LogEntry[]{logEntry});
        ProxyToNodeRequest proxyToNodeRequest = null;
        if (requestCommand.getCommand() instanceof ProxyToNodeRequest) {
            proxyToNodeRequest = (ProxyToNodeRequest) requestCommand.getCommand();
            Kv kv = proxyToNodeRequest.getKv();
            Kv[] kvs = proxyToNodeRequest.getKvs();
            switch (proxyToNodeRequest.getOperation()) {
                case PUT:
                    this.dataStorage.put(kv);
                    break;
                case GET:
                    this.dataStorage.get(kv);
                    break;
                case DELETE:
                    this.dataStorage.delete(kv);
                    break;
                case BATCH_PUT:
                    this.dataStorage.batchPut(kvs);
                    break;
                case BATCH_GET:
                    this.dataStorage.batchGet(kvs);
                    break;
                case BATCH_DELETE:
                    break;
            }
        }
        responseFuture.complete(new ResponseCommand());
    }

    private ConcurrentHashMap<byte[], byte[]> memCache = new ConcurrentHashMap<>();
    private ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void init() {
        this.dataStorage = new DefaultDataStorage(config,trp);
        this.logStorage = new DefaultLogStorage(config,trp);
        // 启动定时刷盘任务
        flushExecutor.scheduleAtFixedRate(() -> {
            synchronized (memCache) {
                dataStorage.batchPut(convertCacheToKvs());
                memCache.clear();
            }
        }, 50, 50, TimeUnit.MILLISECONDS);
    }

    private Kv[] convertCacheToKvs() {
        ConcurrentHashMap<byte[], byte[]> memCache = this.memCache;
        // todo 转换为Kv[] 存储到 logStorage 和 dataStorage
        return new Kv[0];
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        if(queueConsumer != null) {
            queueConsumer.shutdown();
        }
    }

    @Override
    public Endpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public boolean becomeLeader() {
        // 开启复制线程
        this.isLeader = true;
        groups.forEach(endpoint -> {
            if (endpoint.equals(this.endpoint)) {
                return;
            }
            // 启动复制线程
            replicationTasks.add(new TimerTask(() -> {
                new ReplicationsTask(this, endpoint,trpNode);
            }));
            replicationTasks.forEach(timerTask -> timerTask.start(100));
        });
        return true;
    }

    @Override
    public boolean isLeader() {
        return isLeader;
    }

    @Override
    public void transferLeadership() {
        this.isLeader = false;
        // 触发新的选举
        this.term++;
        this.commitIndex.clear();
    }

    @Override
    public LogStorage getLogStorage() {
        return this.logStorage;
    }

    @Override
    public Long getCommitIndex() {
        return null;
    }

    @Override
    public Map<String, Long> getMatchIndex() {
        return null;
    }

    @Override
    public Map<String, Long> getNextIndex() {
        return null;
    }

    @Override
    public long getCurrentTerm() {
        return 0;
    }

    @Override
    public Iterable<LogEntry> iterateLogs() {
        return this.logStorage.iterateLogs();
    }
}
