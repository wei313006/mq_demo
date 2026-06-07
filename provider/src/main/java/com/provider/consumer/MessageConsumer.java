
package com.provider.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(topic = "test-topic", consumerGroup = "test-consumer-group")
public class MessageConsumer implements RocketMQListener<String> {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    @Override
    public void onMessage(String message) {
        logger.info("收到普通消息: {}", message);
    }
}
