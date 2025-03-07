package cn.gm.light.rtable.core.proxy.strategy;

import cn.gm.light.rtable.core.proxy.ShardStrategy;

import java.util.List;

// 可配置的混合策略（组合模式）
public class CompositeShardStrategy implements ShardStrategy {

    private List<ShardStrategy> strategies;
    public CompositeShardStrategy(List<ShardStrategy> strategies) {
        this.strategies = strategies;
    }
    private boolean isValid(String key) {
        return key != null && !key.isEmpty();
    }
    @Override
    public String generateKey(String tableName, String rowKey, String family, String column) {
        for (ShardStrategy strategy : strategies) {
            String key = strategy.generateKey(tableName,  rowKey,  family,  column);
            if (isValid(key)) return key;
        }
        return rowKey;
    }
}