package cn.gm.light.rtable.entity;

import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description 组合key
 * @date 2025/3/4 13:31:25
 */
@Data
public class CombinationKey {
    // 表名
    private String tableName;
    // 业务线
    private String family;
    // 业务线key
    private String key;
    // 业务线column
    private String column;
}
