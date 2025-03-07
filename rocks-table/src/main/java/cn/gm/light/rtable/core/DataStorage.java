package cn.gm.light.rtable.core;

import cn.gm.light.rtable.entity.Kv;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description
 * @date 2025/3/3 19:16:02
 */
public interface DataStorage extends LifeCycle {

    void put(Kv kv);

    void get(Kv kv);

    void delete(Kv kv);

    void batchPut(Kv[] kv);

    void batchGet(Kv[] kv);

    void batchDelete(Kv[] key);
}
