package cn.gm.light.rtable.entity;

import com.alibaba.fastjson2.JSON;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/3 19:17:59
 */
@Data
public class Kv implements Serializable {
    // 不同业务线
    private String family;
    // 不同业务线下的key
    private String key;
    // 不同key下的column
    private String column;
    private Object value;

    public byte[] getKeyBytes() {
        return JSON.toJSONBytes(family +"#"+ key +"#"+ column);
    }
    public byte[] getValueBytes() {
        return JSON.toJSONBytes(value);
    }
}
