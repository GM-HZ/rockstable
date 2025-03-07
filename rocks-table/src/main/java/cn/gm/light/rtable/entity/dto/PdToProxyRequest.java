package cn.gm.light.rtable.entity.dto;

import cn.gm.light.rtable.entity.MateData;
import cn.gm.light.rtable.entity.TRP;
import lombok.Data;

import java.util.Map;

@Data
public class PdToProxyRequest {
    // 元数据版本号（用于并发控制）
    private long schemaVersion;

    // 按表维度的全量元数据（包含分片策略、副本分布等）
    private Map<String, MateData> tableMetadata;

    // 按TRP维度的状态变更（精确到分片副本）
    private Map<TRP, Boolean> trpStatusChanges;

//    // 按节点维度的负载信息（CPU、内存、网络等）
//    private Map<String, NodeLoad> nodeLoadStats;

    // 按表+分片维度的最新路由（替代原有shardDistribution）
    private Map<String, Map<Integer, TRP>> shardRouting;

}