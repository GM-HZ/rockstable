package cn.gm.light.rtable;

import cn.gm.light.rtable.core.LifeCycle;
import cn.gm.light.rtable.core.LogStorage;
import cn.gm.light.rtable.entity.Endpoint;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.entity.RequestCommand;
import cn.gm.light.rtable.entity.ResponseCommand;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 18:01:12
 */
public interface Chunk extends LifeCycle{

    void handleClientRequest(RequestCommand requestCommand, CompletableFuture<ResponseCommand> responseFuture);

    Endpoint getEndpoint();

    boolean becomeLeader();

    boolean isLeader();

    void transferLeadership();

    LogStorage getLogStorage();

    Long getCommitIndex();

    Map<String, Long> getMatchIndex();

    Map<String, Long> getNextIndex();

    long getCurrentTerm();

    Iterable<LogEntry> iterateLogs();
}
