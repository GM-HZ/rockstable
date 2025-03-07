package cn.gm.light.rtable.entity;

import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 */
@Data
public class ResponseSnapshot {
    private long term;
    private long index;
    private boolean success;
}
