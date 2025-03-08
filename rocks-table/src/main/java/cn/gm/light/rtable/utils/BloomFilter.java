package cn.gm.light.rtable.utils;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description
 * @date 2025/3/8 20:31:33
 */
public interface BloomFilter {
    // 批量添加元素（线程安全）
    void addAll(Iterable<byte[]> elements);

    // 添加单个元素（线程安全）
    void add(byte[] element);

    // 检查元素是否存在（线程安全）
    boolean mightContain(byte[] element);
}
