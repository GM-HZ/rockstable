package cn.gm.light.rtable.core.config;

import cn.gm.light.rtable.entity.Endpoint;
import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 09:07:47
 */
@Data
public class Config {
    private String tableName;

    private String dataDir;

    private int proxyPort;

    private int nodePort;

    private int pdPort;
    // 内存分片数量
    private Integer memoryShardNum;

    private Endpoint pdEndpoint;
}
