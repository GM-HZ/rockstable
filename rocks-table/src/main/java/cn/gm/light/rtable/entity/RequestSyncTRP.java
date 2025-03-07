package cn.gm.light.rtable.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/5 06:54:06
 */
@Data
public class RequestSyncTRP {
    private String nodeId;
    private Map<String, List<TRP>> tableTrpMap;
    private Endpoint endpoint;
    private NodeMetrics nodeMetrics;
}
