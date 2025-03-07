package cn.gm.light.rtable.core.proxy.strategy;

import cn.gm.light.rtable.core.proxy.ShardStrategy;

public class RowKeyStrategy implements ShardStrategy {
    @Override
    public String generateKey(String tableName, String rowKey, String family, String column) {
        return rowKey;
    }
}