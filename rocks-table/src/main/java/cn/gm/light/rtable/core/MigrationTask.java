package cn.gm.light.rtable.core;

import cn.gm.light.rtable.Chunk;
import cn.gm.light.rtable.core.config.Config;
import cn.gm.light.rtable.core.queue.*;
import cn.gm.light.rtable.entity.LogEntry;
import cn.gm.light.rtable.entity.TRP;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static cn.gm.light.rtable.Consts.MIGRATION_TOPIC;

public class MigrationTask {
    private final TRP sourceTrp;
    private final TRP targetTrp;
    private final Chunk sourceChunk;
    private final LinkedBlockingQueue<MigrationMessage> migrationQueue;
    private volatile boolean isRunning = true;
    private ExecutorService executor;
    private Config config;
    private TrpNode trpNode;

    public MigrationTask(Config config,TRP sourceTrp, TRP targetTrp, Chunk sourceChunk, String queueEndpoint, TrpNode trpNode) {
        this.config = config;
        this.sourceTrp = sourceTrp;
        this.targetTrp = targetTrp;
        this.sourceChunk = sourceChunk;
        this.trpNode = trpNode;
        this.migrationQueue = new LinkedBlockingQueue<>(10000);
        initializeQueueConnection(queueEndpoint);
    }

    private QueueCustomer queueConsumer;
    private QueueProducer queueProducer;

    private void initializeQueueConnection(String endpoint) {
        // 基于RocketMQ实现队列连接
        try {
            DefaultMQProducer producer = new DefaultMQProducer("migration_group");
            producer.setNamesrvAddr(endpoint);
            producer.start();
            
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("migration_group");
            consumer.setNamesrvAddr(endpoint);
            consumer.subscribe(MIGRATION_TOPIC, "*");
            this.queueConsumer = new RocketMQConsumer(consumer);
            this.queueProducer = new RocketMQProducer(producer);
        } catch (MQClientException e) {
            throw new RuntimeException("Failed to initialize MQ connection", e);
        }
    }

    public void startMigration(Runnable completionCallback) {
        executor = Executors.newFixedThreadPool(2);
        
        // 数据导出线程
        executor.submit(() -> {
            while (isRunning) {
                try {
                    Iterable<LogEntry> logEntries = sourceChunk.iterateLogs();
                    while (logEntries.iterator().hasNext()) {
                        LogEntry logEntry = logEntries.iterator().next();
                        if (logEntry != null) {
                            migrationQueue.put(new MigrationMessage(logEntry, false));
                        } else {
                            migrationQueue.put(MigrationMessage.END_MARKER);
                            break;
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        // 数据导入线程
        executor.submit(() -> {
            try {
                while (isRunning) {
                    MigrationMessage message = migrationQueue.take();
                    if (message == MigrationMessage.END_MARKER) {
                        completionCallback.run();
                        break;
                    }
                    // 数据导入目标chunk
                    Chunk targetChunk = new ChunkImpl(config, targetTrp,trpNode);

                    targetChunk.transferLeadership();  // 迁移前先转移Leader身份
                    Object payload = message.getPayload();
                    targetChunk.getLogStorage().append(new LogEntry[]{(LogEntry) payload});
                    queueProducer.ack(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public void stopMigration() {
        isRunning = false;
        executor.shutdownNow();
    }
}