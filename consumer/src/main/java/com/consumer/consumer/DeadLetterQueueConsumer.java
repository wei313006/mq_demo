package com.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RocketMQMessageListener(topic = "test-dlq-topic", consumerGroup = "test-dlq-consumer-group")
public class DeadLetterQueueConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        log.warn("收到死信队列消息: {}", message);
    }
}
