package cn.gm.light.rtable.entity;

import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 09:05:02
 */
@Data
public class Endpoint {
    private String ip;
    private int port;
    private String addr;

    public Endpoint(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.addr = ip + ":" + port;
    }

    public Endpoint(String addr) {
        this.addr = addr;
        String[] split = addr.split(":");
        this.ip = split[0];
        this.port = Integer.parseInt(split[1]);
    }
}
