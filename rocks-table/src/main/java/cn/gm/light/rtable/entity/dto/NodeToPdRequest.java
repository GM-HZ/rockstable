package cn.gm.light.rtable.entity.dto;

import cn.gm.light.rtable.entity.NodeMetrics;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.enums.CommandType;
import cn.gm.light.rtable.enums.CommonType;
import lombok.Data;

import java.util.List;

@Data
public class NodeToPdRequest {
    private CommandType commandType = CommandType.Node2Pd;
    private CommonType operation;
    
    // 节点状态参数
    private String nodeId;
    private String status;
    private long timestamp;
    private int shardCount;
    private int replicaCount;
    private List<TRP> trps;
    private NodeMetrics nodeMetrics;
}