package com.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
@Component
@RocketMQMessageListener(topic = "test-delay-topic", consumerGroup = "test-delay-consumer-group")
public class DelayedMessageConsumer implements RocketMQListener<String> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void onMessage(String message) {
        log.info("收到延迟消息: {}，当前时间: {}", message, LocalDateTime.now().format(FORMATTER));
    }
}
