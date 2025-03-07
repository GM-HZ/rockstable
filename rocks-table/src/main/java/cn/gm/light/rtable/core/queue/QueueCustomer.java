package cn.gm.light.rtable.core.queue;

// 定义一个接口，包含生产者的基本操作
public interface QueueCustomer {

    /**
     * 启动消息监听/拉取
     * @param messageHandler 消息处理回调
     */
    void start(Runnable messageHandler);

    /**
     * 关闭消息客户端
     */
    void shutdown();

}
