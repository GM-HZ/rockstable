package cn.gm.light.rtable.core.replication;

import cn.gm.light.rtable.Chunk;
import cn.gm.light.rtable.core.TrpNode;
import cn.gm.light.rtable.core.rpc.BoltClient;
import cn.gm.light.rtable.entity.*;
import cn.gm.light.rtable.enums.CommandType;
import cn.gm.light.rtable.exception.RpcException;
import cn.gm.light.rtable.utils.TimerTask;
import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcResponseFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 11:35:01
 */
@Slf4j
public class ReplicationsTask implements Runnable{
    final long MIN_DELAY = 50; // 新增最小延迟保护
    private final Chunk chunk;
    private final TrpNode trpNode;
    private final String currentPeer;
    private final String targetPeer;
    private final AtomicBoolean replicationInProgress = new AtomicBoolean(false);
    private final long replicationTimeoutMax = 600;
    private final long replicationTimeoutMin = 400;
    private final Random random = new Random();
    private final TimerTask replicationTask = new TimerTask(this);

    private volatile int replicationInterval;

    public ReplicationsTask(Chunk chunk, Endpoint peer, TrpNode trpNode) {
        this.chunk = chunk;
        this.targetPeer = peer.getAddr();
        this.currentPeer = chunk.getEndpoint().getAddr();
        this.replicationInterval = 10;
        this.trpNode = trpNode;
    }

    public void start(boolean isDelay) {
        if (isDelay) {
            this.replicationTask.start(getRandomElectionTimeout(), getRandomElectionTimeout());
        } else {
            this.replicationTask.start(MIN_DELAY, getRandomElectionTimeout());
        }
    }

    private long getRandomElectionTimeout() {
        return replicationTimeoutMin + random.nextInt((int) (replicationTimeoutMax - replicationTimeoutMin));
    }

    public void replications() {
        // 修改进入条件检查
        if (!this.chunk.isLeader()) {
            log.debug("不是leader,currentPeer:{},to:{}, stop replications", currentPeer, targetPeer);
            this.replicationTask.stop();
        }
        if (!replicationInProgress.compareAndSet(false, true)) {
            log.debug("replications正在执行中,currentPeer:{},to:{},是否是等待中:{}",
                    chunk.getEndpoint().getAddr(), targetPeer, replicationInProgress.get());
            return;
        }
        try {
            log.debug("开始发送replications日志,currentPeer:{},to:{}", currentPeer, targetPeer);
            LogEntry lastLogEntry = this.chunk.getLogStorage().readLastLog();
            long currentTermTemp = this.chunk.getCurrentTerm();
            Long nextIndex = this.chunk.getNextIndex().get(targetPeer);
            Long matchIndex = this.chunk.getMatchIndex().get(targetPeer);
            log.debug("replications lastIndex:{} nextIndex:{},matchIndex:{},term:{}", lastLogEntry.getIndex(), nextIndex, matchIndex, currentTermTemp);
            log.debug("replications lastIndex:{} nextIndex:{},matchIndex:{}", lastLogEntry.getIndex(), JSON.toJSONString(this.chunk.getNextIndex()), JSON.toJSONString(this.chunk.getMatchIndex()));
            Long index = lastLogEntry.getIndex();
            if (nextIndex == null || nextIndex > index) {  // 修正条件
                log.debug("peer:{}没有日志需要复制", targetPeer);
                return;
            }
            // 动态速度控制
            dynamicSpeedControl(index, nextIndex);

            List<LogEntry> logEntries = this.chunk.getLogStorage().scan(nextIndex, replicationInterval);
            if (logEntries.isEmpty()) {  // 添加空结果处理
                log.debug("没有扫描到需要复制的日志，peer:{}", targetPeer);
                return;
            }
            LogEntry prevLogEntry = this.chunk.getLogStorage().readByIndex(matchIndex);
            LogEntry[] entries = logEntries.toArray(new LogEntry[0]);
            if (prevLogEntry == null) {
                prevLogEntry = new LogEntry();
                prevLogEntry.setIndex(0L);
                prevLogEntry.setTerm(0L);
            }
            Arrays.sort(entries, Comparator.comparingLong(LogEntry::getIndex));
            RequestAppendEntries requestAppendEntries = new RequestAppendEntries();
            requestAppendEntries.setTerm(currentTermTemp);
            requestAppendEntries.setLeaderId(this.chunk.getEndpoint().getAddr());
            requestAppendEntries.setServerId(targetPeer);
            requestAppendEntries.setPrevLogIndex(prevLogEntry.getIndex());
            requestAppendEntries.setPrevLogTerm(prevLogEntry.getTerm());
            requestAppendEntries.setLeaderCommit(this.chunk.getCommitIndex());
            requestAppendEntries.setEntries(entries);

            RequestCommand requestCommand = new RequestCommand();
            requestCommand.setCommandType(CommandType.Node2Node);
            RpcResponseFuture responseFuture = ((BoltClient)trpNode.getClient()).setEndpoint(new Endpoint(targetPeer)).asyncSendRequest(requestCommand);
            log.debug("发送replications日志,currentPeer:{},to:{},request:{}", currentPeer, targetPeer, JSON.toJSONString(requestCommand));
            Object o = responseFuture.get();
            ResponseCommand response = (ResponseCommand) o;
            if (response.getSuccess()){
                ResponseAppendEntries responseAppendEntries = (ResponseAppendEntries) response.getData();
                if (responseAppendEntries.isSuccess()) {

                }
            }
        } catch (RpcException e) {
            log.error("replications异常", e);
            replicationInProgress.set(false);
        } catch (RemotingException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            replicationInProgress.set(false);
            // 新增Leader状态检查
            if (!chunk.isLeader() && replicationTask.isRunning()) {
                replicationTask.stop();
            }
        }
    }

    public void dynamicSpeedControl(Long index, Long nextIndex) {
        // 在 dynamicSpeedControl 方法开头添加：
        if (nextIndex == 0L) {  // 处理初始状态
            replicationInterval = 10;
            return;
        }
        // 优化后的速度控制算法（基于动态积压比例）
        long backlog = index - nextIndex;
        double denominator = Math.max(1, index - this.chunk.getCommitIndex());
        double backlogRatio = (double) backlog / denominator;

        // 动态调整系数（0.5~2.0）
        double factor = Math.min(2.0, Math.max(0.5, 1.0 + Math.log1p(backlogRatio)));

        // 应用指数平滑：new = α * current + (1-α) * previous（α=0.8）
        replicationInterval = (int) (0.8 * replicationInterval * factor + 0.2 * replicationInterval);

        // 最终限制区间（10 ~ 100）
        replicationInterval = Math.min(100, Math.max(10, replicationInterval));
    }

    @Override
    public void run() {
        try {
            replications();
        } catch (Exception e) {
            log.error("replications异常", e);
        }
    }

    public void stop() {
        this.replicationTask.stop();
    }
}
