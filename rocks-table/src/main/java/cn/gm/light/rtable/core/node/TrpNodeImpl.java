package cn.gm.light.rtable.core.node;

import cn.gm.light.rtable.Chunk;
import cn.gm.light.rtable.core.ChunkImpl;
import cn.gm.light.rtable.core.TrpNode;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.rpc.BoltClient;
import cn.gm.light.rtable.core.rpc.BoltServer;
import cn.gm.light.rtable.core.rpc.Client;
import cn.gm.light.rtable.core.rpc.Server;
import cn.gm.light.rtable.entity.*;
import cn.gm.light.rtable.entity.dto.ProxyToNodeRequest;
import cn.gm.light.rtable.enums.CommandType;
import cn.gm.light.rtable.enums.NodeStatus;
import cn.gm.light.rtable.utils.HashUtil;
import cn.gm.light.rtable.utils.IpUtil;
import cn.gm.light.rtable.utils.TimerTask;
import com.alipay.remoting.rpc.RpcResponseFuture;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description chunk集群管理节点
 * @date 2025/3/4 09:03:42
 */
public class TrpNodeImpl implements TrpNode {

    private Endpoint endpoint;

    private String nodeId;

    private Config config;

    private Map<TRP, Chunk> chunks;

    // key: tableName+replicationId+partitionId
    private Map<String, List<TRP>> tableTrpMap;

    private Server server;

    private BoltClient client;

    private NodeStatus status;

    // 最好是节点级别的同步，不然同步数量会太多
    private TimerTask heartbeatTask;

    public TrpNodeImpl(Config config) {
        this.endpoint = new Endpoint(IpUtil.getLocalHostAddress(), config.getNodePort());
        this.nodeId = "node-" + endpoint.getIp() + "-" + endpoint.getPort() + "-" + System.currentTimeMillis() + "-" + HashUtil.hash(endpoint.getIp() + endpoint.getPort());
        this.config = config;
        this.server = new BoltServer(config.getNodePort());
        this.client = new BoltClient();
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public void handleClientRequest(RequestCommand requestCommand, CompletableFuture<ResponseCommand> responseFuture) {
        if (requestCommand.getCommandType() == CommandType.Proxy2Node) {
            // 处理客户端请求
            // 执行相应的操作，并将结果返回给客户端
            // 转发到具体的chunk处理节点
            Object command = requestCommand.getCommand();
            if (command instanceof ProxyToNodeRequest) {
                ProxyToNodeRequest proxyToNodeRequest = (ProxyToNodeRequest) command;
                if (proxyToNodeRequest.isReadOnly()) {
                    // 只读
                    TRP trp = proxyToNodeRequest.getTrp();
                    Chunk chunk = chunks.get(trp);
                    if (chunk == null) {
                        throw new RuntimeException("chunk not found");
                    }
                    chunk.handleClientRequest(requestCommand, responseFuture);
                }
            }
        }if (requestCommand.getCommandType() == CommandType.Pd2Node) {
            // 处理管理中心的请求
//            节点管理
//            初始化 主要是表，分片和副本的初始化信息
//            节点管理 主要是 添加节点，删除节点，更新节点信息，获取节点信息
            Object command = requestCommand.getCommand();
            if (command instanceof ManagerRequest) {
                ManagerRequest managerRequest = (ManagerRequest) command;
                switch (managerRequest.getCommonType()) {
                    case TABLE_CREATE:
                        // 创建表
                        if (!tableTrpMap.containsKey(managerRequest.getTable())) {
//                            已经创建了
                            return;
                        }
                        tableTrpMap.put(managerRequest.getTable(), managerRequest.getTrps());
                        for (TRP trp : managerRequest.getTrps()) {
                            Chunk chunk = chunks.get(trp);
                            if (chunk == null) {
                                // 创建chunk
                                chunk = new ChunkImpl(config, trp, this);
                                chunks.put(trp, chunk);
                            }
                        }
                        break;
                    case TABLE_DROP:
                        // 删除表
                        if (!tableTrpMap.containsKey(managerRequest.getTable())) {
                            return;
                        }
                        for (TRP trp : managerRequest.getTrps()) {
                            Chunk chunk = chunks.get(trp);
                            if (chunk == null) {
                                continue;
                            }
                            chunk.stop();
                            chunks.remove(trp);
                        }
                        tableTrpMap.remove(managerRequest.getTable());
                    case TRP_ADD:
                        // 添加节点
                        // 1. 检查节点是否已经存在
                        // 2. 检查节点是否已经在其他表中
                        // 3. 检查节点是否已经在其他chunk中
                        if (!tableTrpMap.containsKey(managerRequest.getTable())) {
                            return;
                        }
                        List<TRP> trps = tableTrpMap.get(managerRequest.getTable());
                        if (trps.contains(managerRequest.getTrp())) {
                            return;
                        }
                        tableTrpMap.get(managerRequest.getTable()).add(managerRequest.getTrp());
                        if (chunks.containsKey(managerRequest.getTrp())){
                            return;
                        }
                        Chunk chunk = new ChunkImpl(config, managerRequest.getTrp(), this);
                        chunks.put(managerRequest.getTrp(), chunk);
                        break;
                    case TRP_REMOVE:
                        // 删除节点
                        if (!tableTrpMap.containsKey(managerRequest.getTable())) {
                            return;
                        }
                        if (!tableTrpMap.get(managerRequest.getTable()).contains(managerRequest.getTrp())) {
                            return;
                        }
                        tableTrpMap.get(managerRequest.getTable()).remove(managerRequest.getTrp());
                        if (!chunks.containsKey(managerRequest.getTrp())){
                            return;
                        }
                        chunks.get(managerRequest.getTrp()).stop();
                        chunks.remove(managerRequest.getTrp());
                        break;
                    case TRP_TRANSFER:
//                        // 转移节点
//                        if (!tableTrpMap.containsKey(managerRequest.getTable())) {
//                            return;
//                        }
//                        if (!tableTrpMap.get(managerRequest.getTable()).contains(managerRequest.getTrp())) {
//                            return;
//                        }
//                        if (!chunks.containsKey(managerRequest.getTrp())){
//                            return;
//                        }
//
//                        // 暂停接收新请求并启动迁移流程
//                        Chunk oldChunk = chunks.get(managerRequest.getTrp());
//                        oldChunk.pauseProcessing();
//                        // 初始化迁移队列消费者
//                        oldChunk.initQueueConsumer(config.getMigrationQueueEndpoint());
//
//                        // 创建迁移任务
//                        MigrationTask migrationTask = new MigrationTask(
//                                config,
//                                managerRequest.getTrp(),
//                                managerRequest.getTargetTrp(),
//                                oldChunk,
//                                config.getMigrationQueueEndpoint()
//                        );
//
//                        // 启动数据迁移
//                        migrationTask.startMigration(() -> {
//                            // 停止旧队列消费者
//                            oldChunk.stopQueueConsumer();
//
//                            // 等待队列消费完成
//                            while(oldChunk.getQueueSize() > 0) {
//                                Thread.sleep(10);
//                            }
//
//                            // 创建新Chunk并加载迁移数据
//                            Chunk newChunk = new ChunkImpl(config, managerRequest.getTargetTrp());
//                            newChunk.init();
//                            newChunk.importData(oldChunk.exportNextData());
//
//                            chunks.put(managerRequest.getTargetTrp(), newChunk);
//                            newChunk.startProcessingQueue();
//
//                            // 更新元数据
//                            updateMigrationStatus(managerRequest.getTable(),
//                                    managerRequest.getTrp(),
//                                    managerRequest.getTargetTrp());
//
//                            // 清理旧节点
//                            oldChunk.stop();
//                            chunks.remove(managerRequest.getTrp());
//                        });
//                        break;
                    case TRANSFER_LEADER:
                        // 转移leader
                        if (!tableTrpMap.containsKey(managerRequest.getTable())) {
                            return;
                        }
                        if (!tableTrpMap.get(managerRequest.getTable()).contains(managerRequest.getTrp())) {
                            return;
                        }
                        if (!chunks.containsKey(managerRequest.getTrp())){
                            return;
                        }
                        // 启动状态变更，暂停接收请求数据,中间的请求数据存储在queue中，一般这个时间非常短暂，不会影响太久
                    default:
                        break;
                }
            }


            responseFuture.complete(new ResponseCommand());
        }
    }

    @Override
    public void init() {
        this.heartbeatTask = new TimerTask( () -> {
            RequestCommand request = new RequestCommand();
            request.setCommandType(CommandType.Node2Pd);
            RequestSyncTRP requestSyncTRP = new RequestSyncTRP();
            requestSyncTRP.setNodeId(nodeId);
            requestSyncTRP.setTableTrpMap(tableTrpMap);
            request.setCommand(requestSyncTRP);
            RpcResponseFuture rpcResponseFuture = client.setEndpoint(config.getPdEndpoint()).asyncSendRequest(request);
        });
    }

    @Override
    public void start() {
        // 注册节点到pd
        // 更新trp节点信息
        // 启动rpc服务
        // 启动chunk服务
        // 更新节点状态
        // 启动心跳检测
        this.heartbeatTask.start(1000);
        // 启动节点心跳定时任务
//        Timer scheduler;
//        scheduler.scheduleAtFixedRate(() -> {
//            // 上报本节点指标
//            NodeMetrics currentMetrics = new NodeMetrics();
//            currentNode.setNodeMetrics(currentMetrics);
//            etcdStore.registerNode(pdId, currentNode);
//        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {

    }

}
