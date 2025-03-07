package cn.gm.light.rtable.entity;

import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/7 12:39:35
 */
@Data
public class MigrationData {
    // 暂时支持一个 分片的迁移
    private String tableName;
    private TRP sourceTrp;
    private TRP targetTrp;
    private double progress;
    private Boolean isCompleted;
}
