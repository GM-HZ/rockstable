package cn.gm.light.rtable.entity.dto;

import cn.gm.light.rtable.enums.CommandType;
import cn.gm.light.rtable.enums.CommonType;
import cn.gm.light.rtable.entity.TRP;
import lombok.Data;

@Data
public class PdToNodeResponse {
    private CommandType commandType = CommandType.Pd2Node;
    private CommonType operation;
    
    // 迁移指令参数
    private boolean migrationRequired;
    private TRP sourceTrp;
    private TRP targetTrp;
    private String migrationId;
    private int priority;
    // 节点状态确认
    private boolean acknowledged;
}