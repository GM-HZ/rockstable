package cn.gm.light.rtable.core.proxy;

import cn.gm.light.rtable.core.LifeCycle;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.rpc.BoltClient;
import cn.gm.light.rtable.core.rpc.BoltServer;
import cn.gm.light.rtable.core.rpc.DefaultUserProcessor;
import cn.gm.light.rtable.entity.*;
import cn.gm.light.rtable.entity.dto.PdToProxyRequest;
import cn.gm.light.rtable.entity.dto.ProxyToNodeRequest;
import cn.gm.light.rtable.enums.CommandType;
import cn.gm.light.rtable.enums.CommonType;
import cn.gm.light.rtable.enums.NodeRole;
import cn.gm.light.rtable.exception.ClientRequestException;
import cn.gm.light.rtable.utils.HashUtil;
import cn.gm.light.rtable.utils.IpUtil;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcResponseFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 */
@Slf4j
public class ProxyClientImpl implements ProxyClient , LifeCycle {

    private Endpoint endpoint;
    private String proxyId;
    private BoltServer server;
    private BoltClient client;
    private Map<String, MateData> tableMateData;
    private Map<String, Map<Integer, TRP>> shardRouting;
    private long currentVersion;
    private Config config;
    private AtomicLong requestId = new AtomicLong(0);
    public ProxyClientImpl(Config config) {
        this.config = config;
        this.endpoint = new Endpoint(IpUtil.getLocalHostAddress(), config.getProxyPort());
    }


    @Override
    public void init() {
        this.proxyId = "proxy-" + endpoint.getIp() + "-" + endpoint.getPort() + "-" + System.currentTimeMillis() + "-" + HashUtil.hash(endpoint.getIp() + endpoint.getPort());
        this.client = new BoltClient();
        this.server = new BoltServer(endpoint.getPort());
        this.tableMateData = new ConcurrentHashMap<>();
        this.shardRouting = new ConcurrentHashMap<>();
        this.currentVersion = 0;
    }

    @Override
    public void start() {
        this.client.start();
        this.server.start();
        this.server.registerUserProcessor(new DefaultUserProcessor<RequestCommand>() {
            @Override
            public Object handleRequest(BizContext bizCtx, RequestCommand request) throws Exception {
                CommandType commandType = request.getCommandType();
                switch (commandType) {
                    case Pd2Proxy:
                        Object command = request.getCommand();
                        if (command instanceof PdToProxyRequest) {
                            PdToProxyRequest pdToProxyRequest = (PdToProxyRequest) command;
                            if (currentVersion < pdToProxyRequest.getSchemaVersion()){
                                currentVersion = pdToProxyRequest.getSchemaVersion();
                            }else{
                                log.info("currentVersion:{}",currentVersion);
                                return ResponseCommand.ok();
                            }
                            if (tableMateData.isEmpty()) {
                                tableMateData = pdToProxyRequest.getTableMetadata();
                                shardRouting = pdToProxyRequest.getShardRouting();
                            }else{
                                // 增量更新
                                tableMateData.putAll(pdToProxyRequest.getTableMetadata());
                                shardRouting.putAll(pdToProxyRequest.getShardRouting());
                            }
                        }
                        break;
                }
                return ResponseCommand.ok();
            }

            @Override
            public String interest() {
                // 需要制定全限定类名，不然会报找不到处理器的异常
                return RequestCommand.class.getName();
            }
        });
        register(true);
        // 移除原有的etcd元数据监听
        // 改为等待PD节点的主动推送
    }
    @Override
    public Object get(CombinationKey key, Object value) throws ClientRequestException {
        return get(key.getTableName(), key.getFamily(), key.getKey(), key.getColumn());
    }

    @Override
    public Object get(String tableName, String family, String key, String column) throws ClientRequestException {
        TRP trp = calculateTRP(tableName, family, key, column);
        if (trp == null) {
            throw ClientRequestException.of("trp not exist");
        }
        MateData mateData = tableMateData.get(tableName);
        Map<TRP, Endpoint> endpoints = mateData.getTrpEndpointMap();
        // 构建TRP并获取节点
        Endpoint endpoint = endpoints.get(trp);
        if (endpoint == null) {
            throw ClientRequestException.of("endpoint not exist");
        }
        RequestCommand request = new RequestCommand();
        request.setCommandType(CommandType.Proxy2Node);
        Kv kv = new Kv();
        kv.setFamily(family);
        kv.setKey(key);
        kv.setColumn(column);
        ProxyToNodeRequest proxy = ProxyToNodeRequest.builder()
                .proxyId(proxyId)
                .trp(trp)
                .operation(CommonType.GET)
                .kv(kv)
                .version(requestId.incrementAndGet())
                .readOnly(true)
                .build();
        request.setCommand(proxy);
        RpcResponseFuture rpcResponseFuture = client.setEndpoint(endpoint).asyncSendRequest(request);
        try {
            Object o = rpcResponseFuture.get();
            if (o instanceof ResponseCommand) {
                ResponseCommand responseCommand = (ResponseCommand) o;
                return responseCommand.getData();
            }
        } catch (RemotingException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
    
    @Override
    public boolean set(String tableName,String family, String key, String column, Object value) throws ClientRequestException {
        TRP trp = calculateTRP(tableName, family, key, column);
        if (trp == null) {
            throw ClientRequestException.of("trp not exist");
        }
        MateData mateData = tableMateData.get(tableName);
        Map<TRP, Endpoint> endpoints = mateData.getTrpEndpointMap();
        // 构建TRP并获取节点
        Endpoint endpoint = endpoints.get(trp);
        if (endpoint == null) {
            throw ClientRequestException.of("endpoint not exist");
        }
        RequestCommand request = new RequestCommand();
        request.setCommandType(CommandType.Proxy2Node);
        Kv kv = new Kv();
        kv.setFamily(family);
        kv.setKey(key);
        kv.setColumn(column);
        kv.setValue(value);
        ProxyToNodeRequest proxy = ProxyToNodeRequest.builder()
                .proxyId(proxyId)
                .trp(trp)
                .operation(CommonType.PUT)
                .kv(kv)
                .version(requestId.incrementAndGet())
                .readOnly(true)
                .build();
        request.setCommand(proxy);
        RpcResponseFuture rpcResponseFuture = client.setEndpoint(endpoint).asyncSendRequest(request);
        try {
            Object o = rpcResponseFuture.get();
            if (o instanceof ResponseCommand) {
                ResponseCommand responseCommand = (ResponseCommand) o;
                return responseCommand.getSuccess();
            }
        } catch (RemotingException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
    @Override
    public boolean set(CombinationKey key, Object value) throws ClientRequestException {
        return set(key.getTableName(), key.getFamily(), key.getKey(), key.getColumn(), value);
    }

    public TRP calculateTRP(String tableName, String family, String key, String column) {
        MateData mateData = tableMateData.get(tableName);
        if (mateData == null) {
            throw ClientRequestException.of("table not exist");
        }
        // 获取分片计算所需参数
        int partitions = mateData.getPartitions();
        ShardStrategy strategy = mateData.getShardStrategy(); // 改为策略对象
        Map<Integer, TRP> masterReplications = mateData.getMasterReplications();
        // 使用策略模式生成分片键
        String shardKey = strategy.generateKey(tableName, key, family, column);
        int hash = HashUtil.hash(shardKey);
        int partition = hash % partitions; // 改进为取模运算

        // 获取主副本
        return masterReplications.get(partition);
    }

    @Override
    public void stop() {
        // 注册到PD节点
        register(false);
        client.stop();
        server.destroy();
    }
    private void register(boolean register) {
        // 注册到PD节点
        RequestCommand registerCmd = new RequestCommand();
        registerCmd.setCommandType(CommandType.Proxy2Pd);
        RequestRegister build = RequestRegister.builder()
                .endpoint(endpoint)
                .nodeRole(NodeRole.Proxy)
                .nodeId(proxyId)
                .register(register)
                .build();
        registerCmd.setCommand(build);
        client.setEndpoint(config.getPdEndpoint()).asyncSendRequest(registerCmd);
    }

}
