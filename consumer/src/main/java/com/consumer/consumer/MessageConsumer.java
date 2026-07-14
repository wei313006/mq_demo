package com.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RocketMQMessageListener(topic = "test-topic", consumerGroup = "test-consumer-group")
public class MessageConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        log.info("收到普通消息: {}", message);
    }
}
