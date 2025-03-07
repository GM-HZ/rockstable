package cn.gm.light.rtable.enums;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description 集群之间的同步
 * @date 2025/3/4 11:35:01
 */
public enum CommandType {
    Proxy2Pd(1),
    Proxy2Node(2),
    Pd2Proxy(3),
    Pd2Node(4),
    Node2Node(5),
    Node2Pd(6),
    ;
    private int code;
    private CommandType(int code) {
        this.code = code;
    }
    public int getCode() {
        return code;
    }
}
