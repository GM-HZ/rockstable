package cn.gm.light.rtable.core.proxy;

// 新增策略接口（应定义在合适的位置）
public interface ShardStrategy {
    String generateKey(String tableName, String rowKey, String family, String column);
}