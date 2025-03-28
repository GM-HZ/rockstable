package cn.gm.light.rtable.core;

import cn.gm.light.rtable.Chunk;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.queue.QueueCustomer;
import cn.gm.light.rtable.core.replication.ReplicationsTask;
import cn.gm.light.rtable.core.storage.DefaultStorageEngine;
import cn.gm.light.rtable.core.storage.ReplicationEventListener;
import cn.gm.light.rtable.entity.*;
import cn.gm.light.rtable.entity.dto.ProxyToNodeRequest;
import cn.gm.light.rtable.utils.RtThreadFactory;
import cn.gm.light.rtable.utils.TimerTask;
import cn.gm.light.rtable.utils.disruptor.TaskDispatcher;
import cn.gm.light.rtable.utils.disruptor.WaitStrategyType;
import com.alipay.remoting.NamedThreadFactory;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

/**
 * @project JavaStudy
 * @author 明溪
 * @version 1.0
 */
@Slf4j
public class ChunkImpl implements Chunk {
    private String chunkId;
    private TRP trp;
    private Endpoint endpoint;
    private List<Endpoint> groups;
    private StorageEngine storageEngine;
    private Map<String, String> matchIndex;
    private Config config;
    private boolean isLeader;
    private long term;
    private Map<String, Long> commitIndex;
    private Map<String, String> lastApplied;
    private List<TimerTask> replicationTasks;
    private TrpNode trpNode;
    private QueueCustomer queueConsumer;

    private TaskDispatcher kvDispatcher;

    public ChunkImpl(Config config,TRP trp,TrpNode trpNode) {
        this.trp = trp;
        this.config = config;
        this.chunkId = trp.serialize()+"#"+ UUID.randomUUID().toString().replace("-","");
        this.trpNode = trpNode;
        final int numWorkers = Runtime.getRuntime().availableProcessors() << 1;
        final int bufSize = numWorkers << 4;
        final String name = "parallel-kv-executor";
        final ThreadFactory threadFactory = RtThreadFactory.forThreadPool(name);
        this.kvDispatcher = new TaskDispatcher(bufSize, numWorkers, WaitStrategyType.LITE_BLOCKING_WAIT, threadFactory);
    }


    @Override
    public void handleClientRequest(RequestCommand requestCommand, CompletableFuture<ResponseCommand> responseFuture){
        LogEntry logEntry = new LogEntry();
        logEntry.setCommand(requestCommand);
        logEntry.setTerm(term);
        Long append = this.storageEngine.appendLog(logEntry);
        ProxyToNodeRequest proxyToNodeRequest = null;
        if (requestCommand.getCommand() instanceof ProxyToNodeRequest) {
            proxyToNodeRequest = (ProxyToNodeRequest) requestCommand.getCommand();
            Kv kv = proxyToNodeRequest.getKv();
            Kv[] kvs = proxyToNodeRequest.getKvs();
            switch (proxyToNodeRequest.getOperation()) {
                case PUT:
                    this.kvDispatcher.execute(()->this.storageEngine.put(kv));
                    break;
                case GET:
                    this.kvDispatcher.execute(()->this.storageEngine.get(kv));
                    break;
                case DELETE:
                    this.kvDispatcher.execute(()->this.storageEngine.delete(kv));
                    break;
                case BATCH_PUT:
                    this.kvDispatcher.execute(()->this.storageEngine.batchPut(kvs));
                    break;
                case BATCH_GET:
                    this.kvDispatcher.execute(()->this.storageEngine.batchGet(kvs));
                    break;
                case BATCH_DELETE:
                    break;
            }
        }
        responseFuture.complete(new ResponseCommand());
    }


    @Override
    public void init() {
        this.storageEngine = new DefaultStorageEngine(config, chunkId,trp,trpNode);
        // 注册复制事件监听
        storageEngine.registerReplicationListener(new ReplicationEventListener() {
            @Override
            public void onLogAppend(LogEntry entry) {
                // 触发日志复制到从节点,todo 这里应是触发一次
                replicationTasks.forEach(timerTask -> timerTask.start(100));
            }
        });
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
        return this.storageEngine.getShardLogStorage();
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
        return this.storageEngine.getShardLogStorage().iterateLogs();
    }
}
