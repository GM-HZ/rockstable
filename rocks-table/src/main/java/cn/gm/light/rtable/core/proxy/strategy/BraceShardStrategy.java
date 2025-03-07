package cn.gm.light.rtable.core.proxy.strategy;

import cn.gm.light.rtable.core.proxy.ShardStrategy;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// 新增花括号分片策略
public class BraceShardStrategy implements ShardStrategy {
    @Override
    public String generateKey(String tableName, String rowKey, String family, String column) {
        // 使用正则提取花括号内容
        Pattern pattern = Pattern.compile("\\{(.*?)\\}");
        Matcher matcher = pattern.matcher(rowKey);
        if (matcher.find()) {
            return matcher.group(1);
        }
        // 未找到时回退到全key
        return rowKey; 
    }
}