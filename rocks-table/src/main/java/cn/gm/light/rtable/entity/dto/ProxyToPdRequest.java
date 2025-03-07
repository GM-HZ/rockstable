package cn.gm.light.rtable.entity.dto;

import cn.gm.light.rtable.entity.Endpoint;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.enums.CommandType;
import cn.gm.light.rtable.enums.CommonType;
import lombok.Data;

@Data
public class ProxyToPdRequest {
    private CommandType commandType = CommandType.Proxy2Pd;
    private CommonType operation;
    
    // 表操作参数
    private String tableName;
    private int partitions;
    private int replications;
    private String shardStrategy;
    private String shardKey;
    
    // TRP操作参数
    private TRP trp;
    private Endpoint sourceNode;
    private Endpoint targetNode;
}