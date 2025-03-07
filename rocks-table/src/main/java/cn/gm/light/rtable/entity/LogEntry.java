package cn.gm.light.rtable.entity;

import lombok.*;

import java.io.Serializable;

@Data
@AllArgsConstructor
@Builder
@ToString
@NoArgsConstructor
public class LogEntry implements Serializable {
    private Long term;
    private Long index;
    private Object command;
}