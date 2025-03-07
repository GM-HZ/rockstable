package cn.gm.light.rtable.core.queue;

import lombok.Data;

@Data
public class MigrationMessage {
    public static final MigrationMessage END_MARKER = new MigrationMessage(null, true);
    final Object payload;
    final boolean isEnd;

    public MigrationMessage(Object payload, boolean isEnd) {
        this.payload = payload;
        this.isEnd = isEnd;
    }
}