package cn.gm.light.rtable.exception;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 12:59:14
 */
public class RpcException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private int code;
    private String message;
    private Throwable cause;

    // 基础构造方法
    public RpcException(int code, String message) {
        super(message);
        this.code = code;
        this.message = message;
    }
    // 基础构造方法
    public RpcException(String message, Throwable cause) {
        super(message);
        this.message = message;
    }

    // 带异常根源的构造方法
    public RpcException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.message = message;
        this.cause = cause;
    }

    // 快速构建方法（带默认错误码）
    public static RpcException of(String message) {
        return new RpcException(500, message);
    }

    // Getter 方法
    public int getCode() { return code; }
    @Override
    public String getMessage() { return message; }
    @Override
    public Throwable getCause() { return cause; }
}
