package cn.gm.light.rtable.entity;

import lombok.*;

import java.io.Serializable;

@Data
public class LogEntry implements Serializable {
    private Long term;
    private Long index;
    private Object command;
}