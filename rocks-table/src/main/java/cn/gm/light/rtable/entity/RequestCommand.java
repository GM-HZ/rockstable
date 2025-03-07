package cn.gm.light.rtable.entity;

import cn.gm.light.rtable.enums.CommandType;
import lombok.Data;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/3 19:17:27
 */
@Data
public class RequestCommand {
    private Object command;
    private CommandType commandType;

}
