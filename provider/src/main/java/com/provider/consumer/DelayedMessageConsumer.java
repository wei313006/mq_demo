
package com.provider.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

@Component
@RocketMQMessageListener(topic = "test-delay-topic", consumerGroup = "test-delay-consumer-group")
public class DelayedMessageConsumer implements RocketMQListener <String> {

    private static final Logger logger = LoggerFactory.getLogger(DelayedMessageConsumer.class);

    @Override
    public void onMessage(String message) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info("收到延迟消息: {}，当前时间: {}", message, sdf.format(new Date()));
    }
}
