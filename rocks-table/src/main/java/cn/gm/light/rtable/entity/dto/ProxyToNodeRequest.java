package cn.gm.light.rtable.entity.dto;

import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.enums.CommandType;
import cn.gm.light.rtable.enums.CommonType;
import cn.gm.light.rtable.entity.Endpoint;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ProxyToNodeRequest {
    private TRP trp;
    private CommonType operation;
    private String proxyId;
    // 数据路由参数
    private Kv kv;

    private Kv[] kvs;

    // 数据版本控制
    private long version;
    private boolean readOnly;
}