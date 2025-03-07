package cn.gm.light.rtable.entity;

import cn.gm.light.rtable.core.proxy.ShardStrategy;
import lombok.Data;

import java.util.Date;
import java.util.Map;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description
 * @date 2025/3/4 12:30:46
 */
@Data
public class MateData {
    private String tableName;
    // 元数据版本号（用于并发控制）
    private long schemaVersion;
    private int partitions;
    private int replications;
    // 分片策略类型（如 range/hash）
    private ShardStrategy shardStrategy;
    // 分片键定义（用于数据分布）
    // 分片键模式（KV专用格式）示例：
    // "full_key"        -> 使用完整key作为分片依据
    // "prefix:2"        -> 取前2个冒号分隔的部分
    // "regex:(user_\d+)"-> 正则表达式捕获组
    private String shardKey;

    // TRP信息
    private Map<TRP, Endpoint> trpEndpointMap;

    // 增加节点信息
    private Map<String,Endpoint> endpointMap;

    // 增加节点状态统计
    private Map<String, NodeMetrics> nodeStatusStatistics;

    // 增加trp状态统计
    private Map<TRP, Boolean> trpStatus;

    // 分片的主节点
    private Map<Integer,TRP> masterReplications;

    // 表状态（ACTIVE/MAINTENANCE/DELETED）
    private String status;
    // 元数据创建时间
    private Date createTime;
    // 元数据更新时间
    private Date updateTime;
    // TTL（生存时间）用于自动清理
    private Long ttl;
    // 数据校验信息
    private String checksum;
    // 扩展属性（自定义配置）
    private Map<String, Object> extraProps;
}
