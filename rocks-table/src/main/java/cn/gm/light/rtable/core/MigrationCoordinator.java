package cn.gm.light.rtable.core;

import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.pd.EtcdStore;
import cn.gm.light.rtable.entity.MigrationData;
import cn.gm.light.rtable.entity.TRP;
import cn.gm.light.rtable.utils.TimerTask;

import java.util.concurrent.ExecutionException;

/**
 * @author 明溪
 * @version 1.0
 * 选在source节点作为协调者 进行数据迁移
 */
public class MigrationCoordinator {
    private final TRP sourceTrp;
    private final TRP targetTrp;
    private final String queueEndpoint;
    private final TimerTask heartbeatTask;
    private final EtcdStore etcdStore;
    private final Config config;

    public MigrationCoordinator(TRP sourceTrp, TRP targetTrp, String queueEndpoint, Config config, EtcdStore etcdStore) {
        this.sourceTrp = sourceTrp;
        this.targetTrp = targetTrp;
        this.queueEndpoint = queueEndpoint;
        this.heartbeatTask = new TimerTask(this::sendHeartbeat);
        this.etcdStore = etcdStore;
        this.config = config;
    }

    public void startHeartbeat(Runnable completionCallback) {
        heartbeatTask.start(5000); // 每5秒发送心跳
        // 启动迁移状态监控
        new Thread(() -> {
            while (true) {
                checkMigrationStatus(completionCallback);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    private void sendHeartbeat() {
        // 更新etcd中的迁移状态
        MigrationData migrationData = new MigrationData();
        migrationData.setTableName(sourceTrp.getTableName());
        migrationData.setSourceTrp(sourceTrp);
        migrationData.setTargetTrp(targetTrp);
        double migrationProgress = getMigrationProgress();
        migrationData.setProgress(migrationProgress);
        migrationData.setIsCompleted(false);
        if (migrationData.getProgress() >= 1.0) {
            migrationData.setIsCompleted(true);
        }
        etcdStore.saveMigration(sourceTrp.getTableName(), migrationData);
    }

    private void checkMigrationStatus(Runnable callback) {
        MigrationData migration = null;
        try {
            migration = etcdStore.getMigration(sourceTrp.getTableName(), MigrationData.class);
            if (migration.getIsCompleted()){
                heartbeatTask.stop();
                callback.run();
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private double getMigrationProgress() {
        // 实现具体进度计算逻辑
        return 0.0;
    }
}