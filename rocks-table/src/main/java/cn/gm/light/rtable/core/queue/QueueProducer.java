package cn.gm.light.rtable.core.queue;

// 定义一个接口，包含生产者的基本操作
public interface QueueProducer {
    void send(MigrationMessage message);

    void ack(MigrationMessage message);
    void shutdown();
}
