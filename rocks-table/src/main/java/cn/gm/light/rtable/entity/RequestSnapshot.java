package cn.gm.light.rtable.entity;

import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 */
@Data
public class RequestSnapshot {
    private long term;
    private long index;
    private byte[] data;
    private long blockId;
    private long totalBlock;
    // 校验和
    private String checksum;
}
