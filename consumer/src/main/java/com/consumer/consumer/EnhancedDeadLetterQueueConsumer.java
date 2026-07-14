package com.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Slf4j
@Component
@RocketMQMessageListener(
        topic = "%DLQ%test-retry-consumer-group",
        consumerGroup = "dlq-handler-group"
)
public class EnhancedDeadLetterQueueConsumer implements RocketMQListener<MessageExt> {

    private static final String DLQ_TOPIC_PREFIX = "%DLQ%";
    private static final String RETRY_TOPIC = "test-retry-topic";
    private static final String FAILURE_PREFIX = "FAILURE_";
    private static final String RANDOM_FAILURE_PREFIX = "RANDOM_FAILURE_";
    private static final String SUCCESS_AFTER_RETRY_PREFIX = "SUCCESS_AFTER_RETRY_";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final org.slf4j.Logger DLQ_LOGGER = org.slf4j.LoggerFactory.getLogger("DEAD_LETTER_QUEUE");

    @Override
    public void onMessage(MessageExt messageExt) {
        String msgId = messageExt.getMsgId();
        String topic = messageExt.getTopic();
        String originalTopic = getOriginalTopic(topic);
        String messageBody = new String(messageExt.getBody());
        int reconsumeTimes = messageExt.getReconsumeTimes();
        long bornTimestamp = messageExt.getBornTimestamp();
        long storeTimestamp = messageExt.getStoreTimestamp();
        String bornHost = messageExt.getBornHostString();

        LocalDateTime bornTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(bornTimestamp), ZoneId.systemDefault());
        LocalDateTime storeTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(storeTimestamp), ZoneId.systemDefault());

        log.error("========================================");
        log.error("【死信队列告警】收到死信消息");
        log.error("========================================");
        log.error("消息ID: {}", msgId);
        log.error("原始Topic: {}", originalTopic);
        log.error("死信Topic: {}", topic);
        log.error("重试次数: {}", reconsumeTimes);
        log.error("消息内容: {}", messageBody);
        log.error("产生时间: {}", bornTime.format(FORMATTER));
        log.error("存储时间: {}", storeTime.format(FORMATTER));
        log.error("产生主机: {}", bornHost);
        log.error("Tags: {}", messageExt.getTags());
        log.error("Keys: {}", messageExt.getKeys());
        log.error("========================================");

        DLQ_LOGGER.error("DLQ|{}|{}|{}|{}|{}",
                msgId,
                originalTopic,
                reconsumeTimes,
                messageBody,
                LocalDateTime.now().format(FORMATTER));

        handleDeadLetterMessage(messageExt, originalTopic, messageBody, reconsumeTimes);
    }

    private void handleDeadLetterMessage(MessageExt messageExt, String originalTopic,
                                         String messageBody, int reconsumeTimes) {
        log.warn("开始处理死信消息...");

        switch (originalTopic) {
            case RETRY_TOPIC:
                handleRetryTopicDeadLetter(messageBody, reconsumeTimes);
                break;
            default:
                handleGenericDeadLetter(messageExt);
        }

        log.warn("死信消息处理完成");
    }

    private void handleRetryTopicDeadLetter(String messageBody, int reconsumeTimes) {
        log.warn("处理test-retry-topic的死信消息: {}, 重试次数: {}", messageBody, reconsumeTimes);

        if (messageBody.startsWith(FAILURE_PREFIX)) {
            log.warn("分析：这是一个总是失败的测试消息");
        } else if (messageBody.startsWith(RANDOM_FAILURE_PREFIX)) {
            log.warn("分析：这是一个随机失败的测试消息");
        } else if (messageBody.startsWith(SUCCESS_AFTER_RETRY_PREFIX)) {
            log.warn("分析：这是一个重试后应该成功但最终失败的消息");
        }
    }

    private void handleGenericDeadLetter(MessageExt messageExt) {
        log.warn("处理通用死信消息: {}", new String(messageExt.getBody()));
    }

    private String getOriginalTopic(String dlqTopic) {
        if (dlqTopic.startsWith(DLQ_TOPIC_PREFIX)) {
            return dlqTopic.substring(DLQ_TOPIC_PREFIX.length());
        }
        return dlqTopic;
    }
}
