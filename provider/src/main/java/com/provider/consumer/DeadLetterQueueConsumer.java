
package com.provider.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(topic = "test-dlq-topic", consumerGroup = "test-dlq-consumer-group")
public class DeadLetterQueueConsumer implements RocketMQListener<String> {

    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueConsumer.class);

    @Override
    public void onMessage(String message) {
        logger.warn("收到死信队列消息: {}", message);
    }
}
