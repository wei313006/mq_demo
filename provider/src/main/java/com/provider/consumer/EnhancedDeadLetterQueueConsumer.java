
package com.provider.consumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 增强版死信队列消费者
 * 处理进入死信队列的消息，记录详细信息
 */
@Component
@RocketMQMessageListener(
        topic = "%DLQ%test-retry-consumer-group",
        consumerGroup = "dlq-handler-group"
)
public class EnhancedDeadLetterQueueConsumer implements RocketMQListener<MessageExt> {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedDeadLetterQueueConsumer.class);
    private static final Logger dlqLogger = LoggerFactory.getLogger("DEAD_LETTER_QUEUE");

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

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        logger.error("========================================");
        logger.error("【死信队列告警】收到死信消息");
        logger.error("========================================");
        logger.error("消息ID: {}", msgId);
        logger.error("原始Topic: {}", originalTopic);
        logger.error("死信Topic: {}", topic);
        logger.error("重试次数: {}", reconsumeTimes);
        logger.error("消息内容: {}", messageBody);
        logger.error("产生时间: {}", sdf.format(new Date(bornTimestamp)));
        logger.error("存储时间: {}", sdf.format(new Date(storeTimestamp)));
        logger.error("产生主机: {}", bornHost);
        logger.error("Tags: {}", messageExt.getTags());
        logger.error("Keys: {}", messageExt.getKeys());
        logger.error("========================================");

        // 记录到专门的死信日志文件（生产环境建议存储到数据库或告警系统）
        dlqLogger.error("DLQ|{}|{}|{}|{}|{}", 
                msgId, 
                originalTopic, 
                reconsumeTimes, 
                messageBody,
                sdf.format(new Date()));

        // 生产环境可以在这里添加告警逻辑
        // 例如：发送钉钉/企业微信告警、写入数据库、调用运维接口等
        handleDeadLetterMessage(messageExt, originalTopic, messageBody, reconsumeTimes);
    }

    /**
     * 处理死信消息
     * 生产环境中这里可以实现：
     * 1. 存入数据库供后续分析
     * 2. 发送告警通知
     * 3. 人工介入处理接口
     * 4. 自动修复逻辑（如果可能）
     */
    private void handleDeadLetterMessage(MessageExt messageExt, String originalTopic, 
                                         String messageBody, int reconsumeTimes) {
        logger.warn("开始处理死信消息...");
        
        // 场景1：根据原始Topic分发处理
        switch (originalTopic) {
            case "test-retry-topic":
                handleRetryTopicDeadLetter(messageBody, reconsumeTimes);
                break;
            default:
                handleGenericDeadLetter(messageExt);
        }
        
        logger.warn("死信消息处理完成");
    }

    /**
     * 处理test-retry-topic的死信消息
     */
    private void handleRetryTopicDeadLetter(String messageBody, int reconsumeTimes) {
        logger.warn("处理test-retry-topic的死信消息: {}, 重试次数: {}", messageBody, reconsumeTimes);
        
        // 分析失败原因
        if (messageBody.startsWith("FAILURE_")) {
            logger.warn("分析：这是一个总是失败的测试消息");
        } else if (messageBody.startsWith("RANDOM_FAILURE_")) {
            logger.warn("分析：这是一个随机失败的测试消息");
        } else if (messageBody.startsWith("SUCCESS_AFTER_RETRY_")) {
            logger.warn("分析：这是一个重试后应该成功但最终失败的消息");
        }
    }

    /**
     * 处理通用的死信消息
     */
    private void handleGenericDeadLetter(MessageExt messageExt) {
        logger.warn("处理通用死信消息: {}", new String(messageExt.getBody()));
    }

    /**
     * 从死信topic中提取原始topic
     */
    private String getOriginalTopic(String dlqTopic) {
        if (dlqTopic.startsWith("%DLQ%")) {
            return dlqTopic.substring("%DLQ%".length());
        }
        return dlqTopic;
    }
}

