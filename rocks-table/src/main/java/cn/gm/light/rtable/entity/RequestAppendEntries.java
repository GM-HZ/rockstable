package cn.gm.light.rtable.entity;

import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 19:01:37
 */
@Data
public class RequestAppendEntries {
    private long term;
    private String leaderId;
    private String serverId;
    private long prevLogIndex;
    private long prevLogTerm;
    private long leaderCommit;
    private LogEntry[] entries;

    public RequestAppendEntries() {
    }

}
