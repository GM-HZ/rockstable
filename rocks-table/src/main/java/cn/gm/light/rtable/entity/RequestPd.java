package cn.gm.light.rtable.entity;

import cn.gm.light.rtable.core.proxy.ShardStrategy;
import cn.gm.light.rtable.enums.CommonType;
import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/5 09:15:05
 */
@Data
public class RequestPd {
    private String tableName;
    // 元数据版本号（用于并发控制）
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

    // TTL（生存时间）用于自动清理
    private Long ttl;

    private CommonType commonType;
}
