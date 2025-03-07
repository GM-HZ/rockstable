package cn.gm.light.rtable.core.pd;

import cn.gm.light.rtable.entity.MateData;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.PutOption;
import com.alibaba.fastjson.JSON;

import cn.gm.light.rtable.entity.NodeData;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class EtcdStore {
    public static final String LEADER_KEY = "pd/leader";
    private static final String LOCK_PREFIX = "pd/lock/";
    private static final String TABLE_PREFIX = "pd/table/";
    private static final String TRP_PREFIX = "pd/trp/";
    private static final String NODE_PREFIX = "pd/node/";
    private static final String MIGRATION_PREFIX = "pd/migration/";

    private final KV kvClient;
    private final Lease leaseClient;

    public EtcdStore(Client etcdClient) {
        this.kvClient = etcdClient.getKVClient();
        this.leaseClient = etcdClient.getLeaseClient();
    }

    // Leader选举相关
    public CompletableFuture<PutResponse> putWithLease(String key, Object value, long leaseId) {
        return kvClient.put(
            ByteSequence.from(key.getBytes()),
            ByteSequence.from(JSON.toJSONBytes(value)),
            PutOption.newBuilder().withLeaseId(leaseId).build()
        ); 
    }

    // 表元数据操作
    public CompletableFuture<PutResponse> saveTableMeta(String tableName, Object data) {
        return kvClient.put(
            ByteSequence.from((TABLE_PREFIX + tableName).getBytes()),
            ByteSequence.from(JSON.toJSONBytes(data))
        );
    }

    public <T> T getTableMeta(String tableName, Class<T> clazz) throws ExecutionException, InterruptedException {
        GetResponse response = kvClient.get(ByteSequence.from((TABLE_PREFIX + tableName).getBytes())).get();
        return JSON.parseObject(response.getKvs().get(0).getValue().toString(), clazz);
    }

    public Map<String,MateData> getAllTableMeta() throws ExecutionException, InterruptedException {
        GetResponse response = kvClient.get(ByteSequence.from((TABLE_PREFIX).getBytes())).get();
        List<KeyValue> kvs = response.getKvs();
        Map<String,MateData> objectObjectHashMap = new HashMap<>();
        kvs.forEach(x-> {
            try {
                MateData mateData = JSON.parseObject(x.getValue().toString(), MateData.class);
                objectObjectHashMap.put(mateData.getTableName(),mateData);
            } catch (Exception e) {
                log.error("getTableMeta error",e);
            }
        });
        return objectObjectHashMap;
    }

    public List<NodeData> getAllNodeData() throws ExecutionException, InterruptedException {
        GetResponse response = kvClient.get(ByteSequence.from((NODE_PREFIX).getBytes())).get();
        List<KeyValue> kvs = response.getKvs();
        return kvs.stream().map(x-> {
                    try {
                        return JSON.parseObject(x.getValue().toString(), NodeData.class);
                    } catch (Exception e) {
                        return null;
                    }
                }).collect(Collectors.toList());
    }

    public NodeData getNodeData(String nodeIdString) throws ExecutionException, InterruptedException {
        return getAllNodeData().stream().filter(x->x.getNodeId().equals(nodeIdString)).findFirst().get();
    }

    // TRP节点操作
    public CompletableFuture<PutResponse> saveTrpStatus(String trpId, Object status) {
        return kvClient.put(
            ByteSequence.from((TRP_PREFIX + trpId).getBytes()),
            ByteSequence.from(JSON.toJSONBytes(status))
        );
    }

    // 节点注册
    public CompletableFuture<PutResponse> registerNode(String nodeId, NodeData nodeInfo) {
        return kvClient.put(
            ByteSequence.from((NODE_PREFIX + nodeId).getBytes()),
            ByteSequence.from(JSON.toJSONBytes(nodeInfo))
        );
    }

    public <T> T getMigration(String migrationId, Class<T> clazz) throws ExecutionException, InterruptedException {
        GetResponse response = kvClient.get(ByteSequence.from((MIGRATION_PREFIX + migrationId).getBytes())).get();
        return JSON.parseObject(response.getKvs().get(0).getValue().toString(), clazz);
    }

    // 迁移协调
    public CompletableFuture<PutResponse> saveMigration(String migrationId, Object data) {
        return kvClient.put(
            ByteSequence.from((MIGRATION_PREFIX + migrationId).getBytes()),
            ByteSequence.from(JSON.toJSONBytes(data))
        );
    }

    public boolean tryLock(String lockKey, int ttlSeconds) throws Exception {
        long leaseId = createLease(ttlSeconds).get();
        return kvClient.put(
            ByteSequence.from((LOCK_PREFIX + lockKey).getBytes()),
            ByteSequence.from("locked".getBytes()),
            PutOption.newBuilder().withLeaseId(leaseId).build()
        ).get().hasPrevKv();
    }

    public void releaseLock(String lockKey) {
        kvClient.delete(ByteSequence.from((LOCK_PREFIX + lockKey).getBytes()));
    }

    // 通用lease操作
    public CompletableFuture<Long> createLease(int ttlSeconds) throws ExecutionException, InterruptedException {
        return leaseClient.grant(ttlSeconds).thenApply(leaseGrantResponse -> leaseGrantResponse.getID());
    }
}