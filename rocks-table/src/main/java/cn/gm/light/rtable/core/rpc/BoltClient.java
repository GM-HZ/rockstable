package cn.gm.light.rtable.core.rpc;

import cn.gm.light.rtable.core.LifeCycle;
import cn.gm.light.rtable.entity.Endpoint;
import cn.gm.light.rtable.entity.RequestCommand;
import cn.gm.light.rtable.exception.RpcException;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcResponseFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BoltClient implements LifeCycle,Client {
    private final static com.alipay.remoting.rpc.RpcClient CLIENT = new com.alipay.remoting.rpc.RpcClient();

    private final Map<Endpoint, RpcClient> clientMap = new ConcurrentHashMap<>();
    private Endpoint currentEndpoint;

    public BoltClient() {
    }

    @Override
    public void init() throws RpcException {
        CLIENT.init();
    }

    public BoltClient setEndpoint(Endpoint endpoint) {
        this.currentEndpoint = endpoint;
        return this;
    }

    public RpcResponseFuture asyncSendRequest(RequestCommand request) {
        try {
            return CLIENT.invokeWithFuture(currentEndpoint.getAddr(), request, 500);
        } catch (RemotingException e) {
            throw new RpcException("rpc RaftRemotingException ", e);
        } catch (InterruptedException e) {
            // ignore
        }
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        clientMap.clear();
        CLIENT.shutdown();
    }
}