package cn.gm.light.rtable.core.proxy;

import cn.gm.light.rtable.entity.CombinationKey;
import cn.gm.light.rtable.exception.ClientRequestException;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description
 * @date 2025/3/4 13:06:42
 */
public interface ProxyClient {

    Object get(CombinationKey key, Object value) throws ClientRequestException;

    Object get(String tableName, String family, String key, String column) throws ClientRequestException;

    boolean set(String tableName, String family , String key, String column, Object value) throws ClientRequestException;
    boolean set(CombinationKey key, Object value) throws ClientRequestException;
}
