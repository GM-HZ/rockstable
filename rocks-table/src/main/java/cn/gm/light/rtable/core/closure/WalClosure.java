package cn.gm.light.rtable.core.closure;

import cn.gm.light.rtable.Status;
import cn.gm.light.rtable.core.storage.shard.BatchRequestTracker;

/**
 * @author gongmeng
 * @version 1.0
 * @description: TODO
 * @date 2025/3/16 12:02
 */
public class WalClosure implements Closure {
    private final BatchRequestTracker tracker;
    public WalClosure(BatchRequestTracker tracker) {
        this.tracker = tracker;
    }
    @Override
    public void run(Status status) {
        if (status.isOk()) {
            tracker.onShardComplete(null);
        } else {
            tracker.onShardComplete(new Exception(status.getErrorMsg()));
        }
    }
}
