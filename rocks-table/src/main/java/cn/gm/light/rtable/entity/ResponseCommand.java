package cn.gm.light.rtable.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/4 09:24:23
 */
@Data
public class ResponseCommand implements Serializable {
    private Boolean success;
    private Integer code;
    private String msg;
    private Object data;

    // 改为静态工厂方法
    public static ResponseCommand ok(Object data) {
        ResponseCommand response = new ResponseCommand();
        response.setSuccess(true);
        response.setCode(200);
        response.setMsg("success");
        response.setData(data);
        return response;
    }

    public static ResponseCommand ok() {
        return ok(null);
    }

    public static ResponseCommand error(Integer code, String msg) {
        ResponseCommand response = new ResponseCommand();
        response.setSuccess(false);
        response.setCode(code);
        response.setMsg(msg);
        return response;
    }
}
