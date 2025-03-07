package cn.gm.light.rtable.core.pd;

import cn.gm.light.rtable.core.LifeCycle;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.rpc.BoltClient;
import cn.gm.light.rtable.core.rpc.BoltServer;
import cn.gm.light.rtable.core.rpc.DefaultUserProcessor;
import cn.gm.light.rtable.entity.*;
import cn.gm.light.rtable.enums.CommandType;
import cn.gm.light.rtable.enums.CommonType;
import cn.gm.light.rtable.utils.ChecksumUtil;
import com.alibaba.fastjson.JSON;
import com.alipay.remoting.BizContext;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/5 07:05:06
 */
@Slf4j
public class PdNodeImpl implements LifeCycle {

    // 在类成员变量中添加
    private final PriorityQueue<ScheduleTask> migrationQueue = new PriorityQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private Client etcdClient;
    private Lease leaseClient;
    private EtcdStore etcdStore;
    private long leaseId;
    private boolean isLeader = false;

    private Endpoint leaderId;

    private String pdId;

    private BoltClient client;
    private BoltServer server;

    private Config config;

    private List<Endpoint> proxyEndpoints;
    private List<Endpoint> nodeEndpoints;

    public PdNodeImpl(String nodeId, Config config) {
        this.pdId = nodeId;
        this.config = config;
    }

    @Override
    public void init() {
        server = new BoltServer(config.getPdPort());

        etcdClient = Client.builder()
                .endpoints("http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379")
                .build();
        etcdStore = new EtcdStore(etcdClient);
        leaseClient = etcdClient.getLeaseClient();
        // 获取租约并尝试成为Leader
        startLeaderElection();
    }

    @Override
    public void start() {
        
    }

    @Override
    public void stop() {

    }

    private void startLeaderElection() {
        try {
            // 获取10秒租约
            leaseId = leaseClient.grant(10).get().getID();

            // 保持租约活跃
            leaseClient.keepAlive(leaseId, new StreamObserver<LeaseKeepAliveResponse>() {
                @Override
                public void onNext(LeaseKeepAliveResponse value) {
                    // 租约保持活跃
                }

                @Override
                public void onError(Throwable t) {
                    // 处理错误，重新选举
                    startLeaderElection();
                }

                @Override
                public void onCompleted() {
                    // 租约结束，重新选举
                    startLeaderElection();
                }
            });

            // 尝试成为Leader

            etcdStore.putWithLease(EtcdStore.LEADER_KEY, pdId, leaseId).get();
            isLeader = true;
            startLeaderTask();
        } catch (Exception e) {
            isLeader = false;
            startFollowerTask();
        }
    }

    public void handlerClient2PdRequest(RequestCommand requestCommand, CompletableFuture<ResponseCommand> responseFuture) {
        Object command = requestCommand.getCommand();
        if (command instanceof RequestPd) {
            RequestPd requestPd = (RequestPd) command;
            switch (requestPd.getCommonType()) {
                case TABLE_CREATE:
                    // 创建表
                    MateData mateData = new MateData();
                    mateData.setTableName(requestPd.getTableName());
                    mateData.setPartitions(requestPd.getPartitions());
                    mateData.setReplications(requestPd.getReplications());
                    mateData.setShardStrategy(requestPd.getShardStrategy());
                    mateData.setShardKey(requestPd.getShardKey());
                    mateData.setMasterReplications(new HashMap<>());
                    mateData.setTtl(requestPd.getTtl() == null ? 0 : requestPd.getTtl());
                    mateData.setStatus("init");
                    Date createTime = new Date();
                    mateData.setCreateTime(createTime);
                    mateData.setUpdateTime(createTime);
                    mateData.setChecksum(null);
                    mateData.setExtraProps(null);
                    mateData.setSchemaVersion(1L);

                    mateData.setTrpStatus(new HashMap<>());
                    mateData.setNodeStatusStatistics(new HashMap<>());
                    mateData.setTrpEndpointMap(new HashMap<>());
                    mateData.setEndpointMap(new HashMap<>());


                    try {
                        List<NodeData> allNodeData = etcdStore.getAllNodeData();
                        List<Endpoint> endpoints  = allNodeData.stream().map(NodeData::getEndpoint).collect(Collectors.toList());
                        Map<Endpoint, List<TRP> > trps = buildTableTrp(mateData,endpoints);
                        for (Endpoint endpoint : endpoints) {
                            RequestCommand request = new RequestCommand();
                            request.setCommandType(CommandType.Pd2Node);
                            Manager2TRPRequest manager2TRPRequest = new Manager2TRPRequest();
                            manager2TRPRequest.setCommonType(CommonType.TABLE_CREATE);
                            manager2TRPRequest.setTable(requestPd.getTableName());
                            manager2TRPRequest.setReplications(requestPd.getReplications());
                            manager2TRPRequest.setPartitions(requestPd.getPartitions());
                            manager2TRPRequest.setTrps(trps.get(endpoint));
                            request.setCommand(manager2TRPRequest);
                            client.setEndpoint(endpoint).asyncSendRequest(request);
                        }
                        mateData.setChecksum(ChecksumUtil.sha256(JSON.toJSONBytes(mateData)));
                        // 到表的维度进行同步，todo建立watch，监听表成功建立的消息
                        etcdStore.saveTableMeta(mateData.getTableName(), mateData);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case TABLE_DROP:
                    // 删除表
                    // ...
                    break;
            }
        }else if (command instanceof RequestRegister) {
            RequestRegister requestRegister = (RequestRegister) command;
            switch (requestRegister.getCommonType()) {
                case REGISTER:
                    // ...
                    proxyEndpoints.add(requestRegister.getEndpoint());
                    break;
                case UNREGISTER:
                    // ...
                    proxyEndpoints.remove(requestRegister.getEndpoint());
                    break;
            }

        }
    }

    public void handleClientRequest(RequestCommand requestCommand, CompletableFuture<ResponseCommand> responseFuture) {
        if (isLeader) {
            // Leader处理请求
            // ...
            Object command = requestCommand.getCommand();
            switch (requestCommand.getCommandType()) {
                case Proxy2Pd:
                    // 处理客户端请求
                    // ...
                    handlerClient2PdRequest(requestCommand, responseFuture);
                    responseFuture.complete(ResponseCommand.ok());
                    break;
                case Node2Pd:
                    // 处理节点同步请求
                    // ...
                    if (command instanceof RequestSyncTRP) {
                        RequestSyncTRP requestSyncTRP = (RequestSyncTRP) command;
                        Map<String, List<TRP>> tableTrpMap = requestSyncTRP.getTableTrpMap();
                        Endpoint endpoint = requestSyncTRP.getEndpoint();
                        String nodeId = requestSyncTRP.getNodeId();
                        // 更新节点状态,todo 这里应该存储在node基本，不是metadata级别
                        NodeMetrics nodeMetrics = requestSyncTRP.getNodeMetrics();
                        try {
                            // 更新节点状态，更新metadata
                            NodeData nodeData = etcdStore.getNodeData(nodeId);
                            Map<String, MateData> allTableMeta = etcdStore.getAllTableMeta();
                            nodeData.setNodeMetrics(nodeMetrics);
                            nodeData.setUpdateTime(new Date());
                            nodeData.setVersion(nodeData.getVersion() + 1);
                            nodeData.setTableMap(tableTrpMap);
                            // 这里会有多个表
                            for (Map.Entry<String, List<TRP>> entry : tableTrpMap.entrySet()) {
                                etcdStore.getAllNodeData();
                                String tableName = entry.getKey();
                                List<TRP> trps = entry.getValue();
                                MateData mateData = allTableMeta.get(tableName);
                                Map<TRP, Boolean> trpStatus = mateData.getTrpStatus();
                                Map<TRP, Endpoint> trpEndpointMap = mateData.getTrpEndpointMap();
                                for (TRP trp : trps) {
                                    trpStatus.put(trp, true);
                                    trpEndpointMap.put(trp, endpoint);
                                }
                                mateData.getEndpointMap().putIfAbsent(nodeId,endpoint);
                                mateData.setTrpStatus(trpStatus);
                                int count = (int)trpStatus.values().stream().filter(x -> x).count();
                                if (count == mateData.getReplications() * mateData.getPartitions()){
                                    mateData.setStatus("NORMAL");
                                }
                                mateData.setUpdateTime(new Date());
                                mateData.setSchemaVersion(mateData.getSchemaVersion() + 1);
                                mateData.setChecksum(ChecksumUtil.sha256(JSON.toJSONBytes(mateData)));
                                etcdStore.saveTableMeta(mateData.getTableName(), mateData);
                            }
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                    }
                    responseFuture.complete(ResponseCommand.ok());
            }
        } else {
            // Follower转发请求
            // ...
        }
    }

    private Map<Endpoint, List<TRP>> buildTableTrp(MateData mateData, List<Endpoint> endpoints) {
        Map<Endpoint, List<TRP>> tableTrpMap = new HashMap<>();
        endpoints.forEach(endpoint -> {
            List<TRP> trps = new ArrayList<>();
            tableTrpMap.put(endpoint, trps);
        });
        int size = endpoints.size();
        int replications = mateData.getReplications();
        int partitions = mateData.getPartitions();
        Map<Integer, TRP> masterReplications = mateData.getMasterReplications();
        // 修正异常信息
        if (replications > size) {
            throw new RuntimeException("replications > size");
        }
        for (int r = 0; r < replications; r++) {
            for (int p = 0; p < partitions; p++) {
                int index = (r * partitions + p) % size;
                TRP trp = TRP.builder()
                        .tableName(mateData.getTableName())
                        .replicationId(r)
                        .partitionId(p)
                        .term(0)
                        // 这里可以根据实际情况设计更合理的主节点选举逻辑
                        .isLeader(r == 0)
                        .build();
                if (r == 0) {
                    masterReplications.put(p, trp);
                }
                Endpoint endpoint = endpoints.get(index);
                tableTrpMap.get(endpoint).add(trp);
            }
        }
        // 可以在这里添加考虑节点负载的逻辑
        // 例如，获取每个节点的负载信息，然后根据负载信息重新分配
        return tableTrpMap;
    }

    private void startLeaderTask() {
        // Leader需要执行的任务
        // 例如：处理请求、调度任务等
        // 添加etcd监听器
        // 执行调度
        // 调度节点
        this.server.registerUserProcessor(new DefaultUserProcessor<RequestCommand>() {
            @Override
            public Object handleRequest(BizContext bizCtx, RequestCommand request) throws Exception {
                CompletableFuture<ResponseCommand> responseFuture = new CompletableFuture<>();
                handleClientRequest(request, responseFuture);
                return responseFuture.get();
            }
            @Override
            public String interest() {
                return RequestCommand.class.getName();
            }
        });
        this.server.start();
        autoManageNodeTrp();
    }

    private void startFollowerTask() {
        // Follower需要执行的任务
        // 例如：监听Leader状态，准备接管
        scheduler.shutdown();
    }

    public boolean isLeader() {
        return isLeader;
    }

    // 自动管理节点trp
    // 新增调度任务类
    private static class ScheduleTask implements Comparable<ScheduleTask> {
    private MigrationTask task;
    private int priority;
    
    public ScheduleTask(MigrationTask task, int priority) {
        this.task = task;
        this.priority = priority;
    }

    @Override
    public int compareTo(ScheduleTask other) {
        return Integer.compare(other.priority, this.priority);
    }
}

// 新增迁移任务数据结构
private static class MigrationTask {
    String migrationId = UUID.randomUUID().toString();
    long version = 1L;
    Endpoint sourceNode;
    Endpoint targetNode;
    List<TRP> trps;
    String reason;
    MigrationStatus status = MigrationStatus.PENDING;

    enum MigrationStatus {
        PENDING, LOCKED, COMMITTING, ROLLBACK, COMPLETED
    }
}



// 修改autoManageNodeTrp方法
public void autoManageNodeTrp() {




    // 启动定期调度任务
    scheduler.scheduleAtFixedRate(() -> {
        try {
//            // 1. 收集节点负载指标
//            Map<Endpoint, NodeMetrics> metrics = collectNodeMetrics();
//
//            // 2. 负载均衡算法
//            List<MigrationTask> tasks = new LoadBalancer(metrics)
//                .setCpuThreshold(0.8)
//                .setMemThreshold(0.75)
//                .analyze();
//
//            // 3. 任务入队并设置优先级
//            tasks.forEach(task ->
//                migrationQueue.add(new ScheduleTask(task, calculatePriority(task))));
//
//            // 4. 执行迁移任务
//            while (!migrationQueue.isEmpty()) {
//                executeMigration(migrationQueue.poll().task);
//            }
        } catch (Exception e) {
            log.error("TRP调度任务执行失败", e);
        }
    }, 0, 30, TimeUnit.SECONDS);
}

    private int calculatePriority(MigrationTask task) {
        return 0;
    }

    // 新增私有方法
private Map<Endpoint, NodeMetrics> collectNodeMetrics() throws ExecutionException, InterruptedException {
//    Map<Endpoint, NodeMetrics> collect = etcdStore.getAllNodeData().stream()
//            .collect(Collectors.toMap(
//                    NodeData::getEndpoint,
//                    node -> {
//                        NodeMetrics metrics = new NodeMetrics();
//                        return new NodeMetricsData(
//                                metrics.getCpuUsage(),
//                                metrics.getMemUsage(),
//                                metrics.getNetworkLoad()
//                        );
//                    }
//            ));
//    return collect;
    return null;
}

private void executeMigration(MigrationTask task) {
    try {
        // 保存任务初始状态
        etcdStore.saveMigration(task.migrationId, task);

        // 获取分布式锁
        boolean lockAcquired = etcdStore.tryLock("migration_lock", 30);
        if (!lockAcquired) {
            log.warn("获取迁移锁失败，任务重新入队: {}", task.migrationId);
            migrationQueue.add(new ScheduleTask(task, task.trps.size()));
            return;
        }

        task.status = MigrationTask.MigrationStatus.LOCKED;
        etcdStore.saveMigration(task.migrationId, task);

        // 执行迁移（模拟核心逻辑）
        task.status = MigrationTask.MigrationStatus.COMMITTING;
        etcdStore.saveMigration(task.migrationId, task);

        // 错误模拟和回滚
        try {
            // 实际迁移逻辑应在此处实现
            // 1. 通知源节点停止服务对应TRP
            // 2. 复制数据到目标节点
            // 3. 更新元数据
            // 4. 通知代理更新路由

            task.status = MigrationTask.MigrationStatus.COMPLETED;
        } catch (Exception e) {
            log.error("迁移执行失败，开始回滚: {}", task.migrationId, e);
            task.status = MigrationTask.MigrationStatus.ROLLBACK;
            // 执行回滚逻辑
            // 1. 恢复源节点TRP状态
            // 2. 删除目标节点临时数据
            // 3. 恢复元数据版本
        } finally {
            etcdStore.saveMigration(task.migrationId, task);
            etcdStore.releaseLock("migration_lock");
        }
    } catch (Exception e) {
        log.error("迁移任务处理异常: {}", task.migrationId, e);
    }
}

    /**
     * 这里应该是一个任务，定期检测不同节点之间的均衡性，并且进行节点迁移
     * 1. 检测节点之间的负载均衡，网络连接，磁盘空间，内存空间，cpu使用率，网络带宽
     * 2. 依据迁移策略，进行节点迁移
     * 3. 迁移完成后，更新节点信息，通知proxy
     */
    public void manager() {
//      1. 检测节点之间的负载均衡，网络连接，磁盘空间，内存空间，cpu使用率，网络带宽
//      2. 依据迁移策略，进行节点迁移 ,生成迁移任务，可以包含一系列的计划，每一个对应ManagerRequest一个
//      3. 迁移完成后，更新节点信息，通知proxy


        ManagerRequest managerRequest = new ManagerRequest();
    }
}