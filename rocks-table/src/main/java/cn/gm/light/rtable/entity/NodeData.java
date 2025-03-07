package cn.gm.light.rtable.entity;

import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
public class NodeData {
    private String nodeId;
    private Endpoint endpoint;
    private Map<String, List<TRP>> tableMap;
    private NodeMetrics nodeMetrics;
    private Date createTime;
    private Date updateTime;
    private long version;
}
