package cn.gm.light.rtable.entity;

import cn.gm.light.rtable.enums.CommonType;
import cn.gm.light.rtable.enums.NodeRole;
import lombok.Builder;
import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/5 17:44:06
 */
@Data
@Builder
public class RequestRegister {
    private String nodeId;
    private Endpoint endpoint;
    private NodeRole nodeRole;
    private Boolean register;
    private CommonType commonType;
}
