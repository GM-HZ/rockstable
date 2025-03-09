package cn.gm.light.rtable.core.queue;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static cn.gm.light.rtable.Consts.MIGRATION_TOPIC;

// 实现RocketMQ生产者封装类
public class RocketMQProducer implements QueueProducer {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQProducer.class);
    private final DefaultMQProducer producer;


    // 构造函数，初始化生产者
    public RocketMQProducer(DefaultMQProducer producer) {
        this.producer = producer;
        try {
            // 启动生产者
            this.producer.start();
        } catch (MQClientException e) {
            logger.error("Failed to start RocketMQ producer", e);
            throw new RuntimeException("Failed to start RocketMQ producer", e);
        }
    }

    // 发送消息的方法
    @Override
    public void send(MigrationMessage message) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            logger.error("Message or message body is null, cannot send");
            return;
        }
        try {
            Message msg = new Message(MIGRATION_TOPIC, JSON.toJSONBytes(message.getPayload()));
            // 发送消息并获取发送结果
            SendResult sendResult = producer.send(msg);
            logger.info("Message sent successfully, msgId: {}", sendResult.getMsgId());
        } catch (Exception e) {
            logger.error("Message send failed", e);
            throw new RuntimeException("Message send failed", e);
        }
    }

    // 消息确认方法，RocketMQ自动处理
    @Override
    public void ack(MigrationMessage message) {
        // RocketMQ自动处理消息确认

    }

    // 关闭生产者的方法
    @Override
    public void shutdown() {
        if (Objects.nonNull(producer)) {
            producer.shutdown();
            logger.info("RocketMQ producer shutdown");
        }
    }
}