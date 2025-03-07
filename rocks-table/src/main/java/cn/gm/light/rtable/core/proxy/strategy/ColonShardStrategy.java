package cn.gm.light.rtable.core.proxy.strategy;

import cn.gm.light.rtable.core.proxy.ShardStrategy;

// 新增冒号分片策略
public class ColonShardStrategy implements ShardStrategy {
    @Override
    public String generateKey(String tableName, String rowKey, String family, String column) {
        // 取最后一个冒号后的内容
        int lastColonIndex = rowKey.lastIndexOf(':');
        if (lastColonIndex != -1) {
            return rowKey.substring(lastColonIndex + 1);
        }
        // 未找到冒号时回退到全key
        return rowKey;
    }
}