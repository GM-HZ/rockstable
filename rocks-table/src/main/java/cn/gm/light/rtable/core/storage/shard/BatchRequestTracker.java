package cn.gm.light.rtable.core.storage.shard;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
// 请求跟踪器，聚合多个分片的处理结果
public class BatchRequestTracker {
        private final int totalShards;
        private final AtomicInteger completedShards = new AtomicInteger(0);
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        private volatile Throwable failureCause;

        BatchRequestTracker(int totalShards) {
            this.totalShards = totalShards;
        }

        // 分片完成时调用（成功或失败）
        public void onShardComplete(Throwable error) {
            if (error != null) {
                // 仅记录第一个错误
                if (failureCause == null) {
                    failureCause = error;
                    future.completeExceptionally(error);
                }
            } else {
                if (completedShards.incrementAndGet() == totalShards && failureCause == null) {
                    future.complete(null); // 全部分片成功
                }
            }
        }

        public CompletableFuture<Void> getFuture() {
            return future;
        }
    }