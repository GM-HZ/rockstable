package cn.gm.light.rtable.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author gongmeng
 * @version 1.0
 * @description: TODO
 */
@Data
public class ResponseAppendEntries implements Serializable {
    private Integer code;
    private String message;
    private boolean success;
    private long term;
    /**
     * 最后一个日志的索引，leader用这个更新matchIndex
     */
    private long lastLogIndex;
    /**
     * follow的commitIndex
     */
    private long commitIndex;

    public ResponseAppendEntries() {
    }

    public ResponseAppendEntries(boolean success, long term, long lastLogIndex, long commitIndex) {
        this.term = term;
        this.success = success;
        this.lastLogIndex = lastLogIndex;
        this.commitIndex = commitIndex;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
