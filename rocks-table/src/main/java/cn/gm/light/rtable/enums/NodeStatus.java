package cn.gm.light.rtable.enums;

public enum NodeStatus {
    Leader(1),
    Follower(2),
    Pending(3)
    ;

    private final int code;

    NodeStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
