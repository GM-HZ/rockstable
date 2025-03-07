package cn.gm.light.rtable.core.queue;


import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

@Slf4j
public class RocketMQConsumer implements QueueCustomer {
    private final DefaultMQPushConsumer consumer;

    // 修改构造函数
    public RocketMQConsumer(DefaultMQPushConsumer consumer) throws MQClientException {
        this.consumer = consumer;
    }

    @Override
    public void start(Runnable messageHandler) {
        // 1. 先注册监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            messageHandler.run();
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 2. 最后启动消费者
        try {
            consumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException("Consumer startup failed", e);
        }
    }

    @Override
    public void shutdown() {
        this.consumer.shutdown();
    }

}