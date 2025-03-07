/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.gm.light.rtable.core.rpc;


import cn.gm.light.rtable.entity.RequestCommand;
import cn.gm.light.rtable.entity.ResponseCommand;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.RemotingServer;
import com.alipay.remoting.rpc.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BoltServer extends RpcServer implements Server{

    private static final Logger LOGGER     = LoggerFactory.getLogger(BoltServer.class);

    /**
     * 是否已经启动
     */
    protected volatile boolean     started;

    /**
     * Bolt服务端
     */
    protected RemotingServer remotingServer;

    private RpcServer rpcServer;
    private boolean running;

    public BoltServer(int port) {
        rpcServer = new RpcServer(port);
        rpcServer.registerUserProcessor(new DefaultUserProcessor<RequestCommand>() {

            @Override
            public Object handleRequest(BizContext bizContext, RequestCommand requestCommand) throws Exception {
                return handlerRequest(requestCommand);
            }

            @Override
            public String interest() {
                return null;
            }
        });
    }

    public ResponseCommand handlerRequest(RequestCommand request) {
//        if (request.getCommandType() == CommandType.ManagerRequest) {
//            return RpcResponse.success(new RequestVoteFunction(node).apply((RequestVote) request.getRequestData()));
//        } else if (request.getRequestDataType() == RequestAppendEntries.class) {
//            return RpcResponse.success(new AppendEntriesFunction(node).apply((RequestAppendEntries) request.getRequestData()));
//        } else if (request.getRequestDataType() == BusinessRequest.class) {
//            return RpcResponse.success(new BusinessFunction(node).apply((BusinessRequest) request.getRequestData()));
//        }
//        else if (request.getCommand().getCode() == RpcRequest.CHANGE_CONFIG_ADD) {
//            return RpcResponse.success(node.handlerChangeConfig((Peer) request.getData()));
//        }else if (request.getCommand().getCode() == RpcRequest.CHANGE_CONFIG_REMOVE) {
//            return RpcResponse.success(node.handlerRemoveConfig((Peer) request.getData()));
//        }
        return null;
    }

    @Override
    public void init() {
        rpcServer.start();
        this.running = true;
    }


    public void destroy() {
        rpcServer.stop();
        this.running = false;
    }

    public boolean isRunning() {
        return this.running;
    }
}
