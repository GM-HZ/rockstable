package cn.gm.light.rtable.entity;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author 明溪
 * @version 1.0
 * @project JavaStudy
 * @description TODO
 * @date 2025/3/3 19:08:52
 */
@Data
@Builder
public class TRP implements Serializable {
    private static final long serialVersionUID = 1123L;

    private String tableName;
    private int replicationId;
    private int partitionId;

    private boolean isLeader;
    private int term;


    public String serialize() {
        return tableName+"_"+ replicationId +"_"+partitionId;
    }
    public static TRP deserialize(String s) {
        String[] ss = s.split("_");
        return TRP.builder().tableName(ss[0]).replicationId(Integer.parseInt(ss[1])).partitionId(Integer.parseInt(ss[2])).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TRP trp = (TRP) o;
        return replicationId == trp.replicationId && partitionId == trp.partitionId && Objects.equals(tableName, trp.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, replicationId, partitionId);
    }
}
