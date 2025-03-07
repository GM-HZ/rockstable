package cn.gm.light.rtable.entity;

import cn.gm.light.rtable.enums.CommonType;
import lombok.Data;

import java.util.List;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/5 06:15:56
 */
@Data
public class ManagerRequest {
    private CommonType commonType;
    /**
     * 表级别操作,具体的分片应该是在pd中维护的
     */
    private String table;
    private int replications;
    private int partitions;
    private List<TRP> trps;
    /**
     * trp级别操作
     * 从old节点迁移到new节点
     * 状态变更，leader 状态变更，follower状态变更
     */
    private TRP trp;
    private Endpoint oldEndpoint;
    private String oldChunkId;
    private Endpoint newEndpoint;
    private String newChunkId;
    private String leaderChunkId;

    


}
