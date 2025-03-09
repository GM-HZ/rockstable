package cn.gm.light.rtable.core.storage;

import cn.gm.light.rtable.entity.Kv;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.utils.BloomFilter;
import cn.gm.light.rtable.utils.ConcurrentBloomFilter;
import com.alibaba.fastjson2.JSON;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/9 10:45:17
 */
public class Test {
    @org.junit.Test
    public void test() {
        Kv k = new Kv();
        k.setFamily("f");
        k.setKey("key1");
        k.setColumn("c");
        k.setValue("value1");
        System.out.println(k.getKeyBytes());
        byte[] jsonBytes = JSON.toJSONBytes(k);
        System.out.println(jsonBytes);
        LogEntry logEntry = new LogEntry();
        logEntry.setCommand(k);
        logEntry.setIndex(1L);
        logEntry.setTerm(1L);
        byte[] jsonBytes1 = JSON.toJSONBytes(logEntry);
        System.out.println(JSON.toJSONString(jsonBytes1)   );


    }

    @org.junit.Test
    public void test1() {
        BloomFilter bloomFilter = new ConcurrentBloomFilter(1000000, 0.01);
        for (int i = 0; i < 10; i++) {
            bloomFilter.add(String.valueOf(i).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            boolean mightContain = bloomFilter.mightContain(String.valueOf(i).getBytes());
            System.out.println(mightContain);
        }
    }
    // 测试用例
    @org.junit.Test
    public void testBloomFilter() {
        BloomFilter filter = new ConcurrentBloomFilter(1000000, 0.01);

        byte[] existKey = "key1".getBytes();
        filter.add(existKey);

        assertTrue(filter.mightContain(existKey)); // 应返回true
        assertFalse(filter.mightContain("key2".getBytes())); // 应返回false
    }
    // 测试边界值
    @org.junit.Test
    public void testNegativeHash() {
        BloomFilter filter = new ConcurrentBloomFilter(1000, 0.01);
        // 生成强制负哈希值的测试数据
        byte[] data = new byte[]{(byte) 0xFF, (byte) 0xFF};
        filter.add(data); // 原代码会抛出异常，修改后应正常执行
        assertTrue(filter.mightContain(data));
    }

}
