package cn.gm.light.rtable.enums;

public enum CommonType {
    // 客户端请求
    // 分为表级别操作和分片级别操作
    // 表支持的操作有创建表、删除表
    // 分片只支持迁移分片，不支持增加和删减分片，只支持自动扩容和缩容
    TABLE_CREATE,
    TABLE_DROP,

    // kv 操作
    GET,
    PUT,
    DELETE,
    BATCH_PUT,
    BATCH_GET,
    BATCH_DELETE,

    // 节点注册
    REGISTER,
    UNREGISTER,

    // pd 调度trp
    TRP_ADD,
    TRP_REMOVE,
    TRP_TRANSFER,
    // 迁移leader
    TRANSFER_LEADER,

    // 复制
    TRP_SYNC_DATALOG,
    TRP_SYNC_SNAPSHOT,

    // trp同步元数据
    TRP_SYNC_METADATA_PD,


    ;
}
