package cn.gm.light.rtable.utils;

import java.net.*;
import java.util.Collections;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/6 21:59:12
 */
public class IpUtil {
    // 新增私有方法获取本机IP
    public static String getLocalHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            // 回退方案：尝试通过网卡获取
            try {
                return Collections.list(NetworkInterface.getNetworkInterfaces())
                        .stream()
                        .flatMap(ni -> Collections.list(ni.getInetAddresses()).stream())
                        .filter(ia -> !ia.isLoopbackAddress() && ia instanceof Inet4Address)
                        .map(InetAddress::getHostAddress)
                        .findFirst()
                        .orElse("127.0.0.1");
            } catch (SocketException ex) {
                return "127.0.0.1";
            }
        }
    }
}
