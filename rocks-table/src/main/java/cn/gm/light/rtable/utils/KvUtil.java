package cn.gm.light.rtable.utils;

import cn.gm.light.rtable.entity.Kv;
import com.alibaba.fastjson2.JSON;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/9 10:39:35
 */
public class KvUtil {
    public static byte[] getKeyBytes(Kv kv) {
        return JSON.toJSONBytes(kv.getFamily() +"#"+ kv.getKey() +"#"+ kv.getColumn());
    }
}
