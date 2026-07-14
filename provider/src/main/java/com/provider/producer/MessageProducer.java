package com.provider.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessageProducer {

    private final RocketMQTemplate rocketMQTemplate;

    public MessageProducer(RocketMQTemplate rocketMQTemplate) {
        this.rocketMQTemplate = rocketMQTemplate;
    }

    public static final String TOPIC_NORMAL = "test-topic";
    public static final String TOPIC_DELAY = "test-delay-topic";
    public static final String TOPIC_DLQ = "test-dlq-topic";
    public static final String TOPIC_RETRY = "test-retry-topic";

    public static final String FAILURE_PREFIX = "FAILURE_";
    public static final String RANDOM_FAILURE_PREFIX = "RANDOM_FAILURE_";
    public static final String SUCCESS_AFTER_RETRY_PREFIX = "SUCCESS_AFTER_RETRY_";

    private static final int[] DELAY_LEVEL_SECONDS = {1, 5, 10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};
    private static final long SEND_TIMEOUT = 3000;

    public void sendMessage(String message) {
        rocketMQTemplate.convertAndSend(TOPIC_NORMAL, message);
        log.info("发送普通消息成功, topic: {}, message: {}", TOPIC_NORMAL, message);
    }

    public void sendDelayMessage(String message, int delaySeconds) {
        int delayLevel = getDelayLevel(delaySeconds);
        Message<String> msg = MessageBuilder.withPayload(message).build();
        SendResult result = rocketMQTemplate.syncSend(TOPIC_DELAY, msg, SEND_TIMEOUT, delayLevel);
        log.info("发送延迟消息成功, topic: {}, delayLevel: {}, msgId: {}, message: {}",
                TOPIC_DELAY, delayLevel, result.getMsgId(), message);
    }

    public void sendDlqMessage(String message, String reason) {
        String dlqMessage = String.format("Reason: %s | Original: %s", reason, message);
        rocketMQTemplate.convertAndSend(TOPIC_DLQ, dlqMessage);
        log.info("发送死信消息成功, topic: {}, reason: {}", TOPIC_DLQ, reason);
    }

    public String sendAlwaysFailureMessage(String message) {
        String fullMessage = FAILURE_PREFIX + message;
        rocketMQTemplate.convertAndSend(TOPIC_RETRY, fullMessage);
        log.info("发送总是失败消息成功, topic: {}, message: {}", TOPIC_RETRY, fullMessage);
        return fullMessage;
    }

    public String sendRandomFailureMessage(String message) {
        String fullMessage = RANDOM_FAILURE_PREFIX + message;
        rocketMQTemplate.convertAndSend(TOPIC_RETRY, fullMessage);
        log.info("发送随机失败消息成功, topic: {}, message: {}", TOPIC_RETRY, fullMessage);
        return fullMessage;
    }

    public String sendSuccessAfterRetryMessage(String message, int retryTimes) {
        String fullMessage = SUCCESS_AFTER_RETRY_PREFIX + retryTimes + "_" + message;
        rocketMQTemplate.convertAndSend(TOPIC_RETRY, fullMessage);
        log.info("发送重试后成功消息成功, topic: {}, retryTimes: {}, message: {}", TOPIC_RETRY, retryTimes, fullMessage);
        return fullMessage;
    }

    public String sendNormalRetryMessage(String message) {
        rocketMQTemplate.convertAndSend(TOPIC_RETRY, message);
        log.info("发送重试topic正常消息成功, topic: {}, message: {}", TOPIC_RETRY, message);
        return message;
    }

    private int getDelayLevel(int delaySeconds) {
        for (int i = 0; i < DELAY_LEVEL_SECONDS.length; i++) {
            if (delaySeconds <= DELAY_LEVEL_SECONDS[i]) {
                return i + 1;
            }
        }
        return DELAY_LEVEL_SECONDS.length;
    }
}
