package cn.gm.light.rtable.entity;

import com.alibaba.fastjson.JSON;
import lombok.Builder;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/3 19:17:59
 */
@Builder
public class Kv {
    private String family;
    private String key;
    private String column;
    private Object value;

    public byte[] getKeyBytes() {
        return (family +"#"+ key +"#"+ column).getBytes();
    }
    public byte[] getValueBytes() {
        return JSON.toJSONBytes(value);
    }
}
