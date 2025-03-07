package cn.gm.light.rtable.core;

import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.utils.Pair;

import java.util.List;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description
 * @date 2025/3/3 19:16:02
 */
public interface DataStorage extends LifeCycle {

    boolean put(Kv kv);
    byte[] get(Kv kv);
    void delete(Kv kv);


    byte[] get(byte[] k);
    boolean put(Pair<byte[], byte[]> kv);
    boolean delete(byte[] k);
    boolean batchPut(List<Pair<byte[], byte[]>> kvs);
    List<Pair<byte[], byte[]>> batchGet(List<byte[]> k);
    boolean batchDelete(List<byte[]> k);


}
