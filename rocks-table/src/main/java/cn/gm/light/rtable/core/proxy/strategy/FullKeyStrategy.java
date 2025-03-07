package cn.gm.light.rtable.core.proxy.strategy;

import cn.gm.light.rtable.core.proxy.ShardStrategy;

// 示例策略实现（可扩展）
public class FullKeyStrategy implements ShardStrategy {
    @Override
    public String generateKey(String tableName, String rowKey, String family, String column) {
        return tableName + "#" + rowKey + "#" + family + "#" + column;
    }
}