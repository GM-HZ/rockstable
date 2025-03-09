package cn.gm.light.rtable.entity;

import lombok.*;

import java.io.Serializable;

@Data
public class LogEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long term;
    private Long index;
    private Object command;
}