package cn.gm.light.rtable.entity;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

public class MetadataUpdate {
    @JSONField(ordinal = 1)
    private long version;
    @JSONField(ordinal = 2)
    private List<MetadataChange> changes;
    @JSONField(ordinal = 3)
    private boolean fullSync;

    public MetadataUpdate() {}

    public MetadataUpdate(long version, List<MetadataChange> changes, boolean fullSync) {
        this.version = version;
        this.changes = changes;
        this.fullSync = fullSync;
    }

    // getters and setters
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
    public List<MetadataChange> getChanges() { return changes; }
    public void setChanges(List<MetadataChange> changes) { this.changes = changes; }
    public boolean isFullSync() { return fullSync; }
    public void setFullSync(boolean fullSync) { this.fullSync = fullSync; }
}