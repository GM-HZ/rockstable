package cn.gm.light.rtable.utils;

// 新增辅助类（放在同一文件或单独文件）
public class Pair<K, V> {
    private final K key;
    private final V value;
    
    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }
    
    public K getKey() { return key; }
    public V getValue() { return value; }
}