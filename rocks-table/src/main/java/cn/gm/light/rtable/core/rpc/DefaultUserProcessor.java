package cn.gm.light.rtable.core.rpc;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AbstractUserProcessor;

/**
 * @author gongmeng
 * @version 1.0
 * @description:
 */
public abstract class DefaultUserProcessor<T> extends AbstractUserProcessor<T> {

    @Override
    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, T request) {
        throw new UnsupportedOperationException(
                "Raft Server not support handleRequest(BizContext bizCtx, AsyncContext asyncCtx, T request) ");
    }


//    @Override
//    public String interest() {
//        return RpcRequest.class.getName();
//    }

}
