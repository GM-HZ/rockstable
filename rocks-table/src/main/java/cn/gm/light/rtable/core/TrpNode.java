package cn.gm.light.rtable.core;

import cn.gm.light.rtable.core.rpc.Client;
import cn.gm.light.rtable.entity.RequestCommand;
import cn.gm.light.rtable.entity.ResponseCommand;

import java.util.concurrent.CompletableFuture;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description chunk node，存储分片数据，对应一个分片
 * @date 2025/3/4 09:03:14
 */
public interface TrpNode extends LifeCycle{

    Client getClient();

    void handleClientRequest(RequestCommand requestCommand, CompletableFuture<ResponseCommand> responseFuture);


}
