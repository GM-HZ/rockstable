package cn.gm.light.rtable.entity;

import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/6 21:32:34
 */
@Data
public class BaseResponse {
    private boolean success;
    private String message;
    private int code;
    private Object data;
}
