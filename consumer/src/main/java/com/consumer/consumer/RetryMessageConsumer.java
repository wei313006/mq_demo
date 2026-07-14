package com.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
@Component
@RocketMQMessageListener(
        topic = "test-retry-topic",
        consumerGroup = "test-retry-consumer-group",
        consumeMode = ConsumeMode.ORDERLY,
        messageModel = MessageModel.CLUSTERING
)
public class RetryMessageConsumer implements RocketMQListener<MessageExt> {

    private static final String FAILURE_PREFIX = "FAILURE_";
    private static final String RANDOM_FAILURE_PREFIX = "RANDOM_FAILURE_";
    private static final String SUCCESS_AFTER_RETRY_PREFIX = "SUCCESS_AFTER_RETRY_";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final double FAILURE_RATE_THRESHOLD = 0.7;
    private static final int DEFAULT_EXPECTED_RETRIES = 2;

    @Override
    public void onMessage(MessageExt messageExt) {
        String messageBody = new String(messageExt.getBody());
        String msgId = messageExt.getMsgId();
        int reconsumeTimes = messageExt.getReconsumeTimes();

        log.info("========== 收到消息 ==========");
        log.info("消息ID: {}, 重试次数: {}, 消息内容: {}, 接收时间: {}",
                msgId, reconsumeTimes, messageBody, LocalDateTime.now().format(FORMATTER));

        try {
            if (messageBody.startsWith(FAILURE_PREFIX)) {
                handleAlwaysFailure(messageBody, reconsumeTimes);
            } else if (messageBody.startsWith(RANDOM_FAILURE_PREFIX)) {
                handleRandomFailure(messageBody, reconsumeTimes);
            } else if (messageBody.startsWith(SUCCESS_AFTER_RETRY_PREFIX)) {
                handleSuccessAfterRetry(messageBody, reconsumeTimes);
            } else {
                handleNormalMessage(messageBody);
            }

            log.info("消息消费成功: {}", msgId);
        } catch (Exception e) {
            log.error("消息消费失败，触发重试机制, msgId: {}", msgId, e);
            throw new RuntimeException("消费失败，触发重试", e);
        }
    }

    private void handleAlwaysFailure(String message, int reconsumeTimes) {
        log.warn("【总是失败场景】当前重试次数: {}, 消息: {}", reconsumeTimes, message);
        throw new RuntimeException("模拟总是失败的业务异常");
    }

    private void handleRandomFailure(String message, int reconsumeTimes) {
        double random = Math.random();
        log.warn("【随机失败场景】随机值: {}, 当前重试次数: {}, 消息: {}",
                random, reconsumeTimes, message);

        if (random > FAILURE_RATE_THRESHOLD) {
            throw new RuntimeException("模拟随机失败: " + random);
        }
    }

    private void handleSuccessAfterRetry(String message, int reconsumeTimes) {
        int expectedRetries = DEFAULT_EXPECTED_RETRIES;
        try {
            String[] parts = message.split("_");
            if (parts.length >= 4) {
                expectedRetries = Integer.parseInt(parts[3]);
            }
        } catch (NumberFormatException e) {
            log.warn("解析重试次数失败，使用默认值: {}", DEFAULT_EXPECTED_RETRIES, e);
        }

        log.warn("【重试后成功场景】期望重试次数: {}, 当前重试次数: {}, 消息: {}",
                expectedRetries, reconsumeTimes, message);

        if (reconsumeTimes < expectedRetries) {
            throw new RuntimeException("模拟临时故障，还需重试: " + (expectedRetries - reconsumeTimes) + "次");
        }
    }

    private void handleNormalMessage(String message) {
        log.info("【正常消费】处理消息: {}", message);
    }
}
