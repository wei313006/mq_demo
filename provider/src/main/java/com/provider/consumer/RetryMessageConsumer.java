
package com.provider.consumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 带有重试机制的消息消费者
 * 模拟各种消费失败场景
 */
@Component
@RocketMQMessageListener(
        topic = "test-retry-topic",
        consumerGroup = "test-retry-consumer-group",
        consumeMode = ConsumeMode.ORDERLY,
        messageModel = MessageModel.CLUSTERING
)
public class RetryMessageConsumer implements RocketMQListener<MessageExt> {

    private static final Logger logger = LoggerFactory.getLogger(RetryMessageConsumer.class);

    // 模拟失败的开关，通过消息内容控制
    private static final String FAILURE_PREFIX = "FAILURE_";
    private static final String RANDOM_FAILURE_PREFIX = "RANDOM_FAILURE_";
    private static final String SUCCESS_AFTER_RETRY_PREFIX = "SUCCESS_AFTER_RETRY_";

    // 记录重试次数（这里简化处理，实际生产中应该用数据库或Redis存储）
    private static int retryCounter = 0;

    @Override
    public void onMessage(MessageExt messageExt) {
        String messageBody = new String(messageExt.getBody());
        String msgId = messageExt.getMsgId();
        int reconsumeTimes = messageExt.getReconsumeTimes();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        
        logger.info("========== 收到消息 ==========");
        logger.info("消息ID: {}, 重试次数: {}, 消息内容: {}, 接收时间: {}", 
                msgId, reconsumeTimes, messageBody, sdf.format(new Date()));

        try {
            // 根据消息内容模拟不同的消费场景
            if (messageBody.startsWith(FAILURE_PREFIX)) {
                handleAlwaysFailure(messageBody, reconsumeTimes);
            } else if (messageBody.startsWith(RANDOM_FAILURE_PREFIX)) {
                handleRandomFailure(messageBody, reconsumeTimes);
            } else if (messageBody.startsWith(SUCCESS_AFTER_RETRY_PREFIX)) {
                handleSuccessAfterRetry(messageBody, reconsumeTimes);
            } else {
                handleNormalMessage(messageBody);
            }
            
            logger.info("消息消费成功: {}", msgId);
        } catch (Exception e) {
            logger.error("消息消费失败，触发重试机制", e);
            throw new RuntimeException("消费失败，触发重试", e);
        }
    }

    /**
     * 场景1：总是失败的消息
     * 这种消息会不断重试，直到达到最大重试次数后进入死信队列
     */
    private void handleAlwaysFailure(String message, int reconsumeTimes) {
        logger.warn("【总是失败场景】当前重试次数: {}, 消息: {}", reconsumeTimes, message);
        throw new RuntimeException("模拟总是失败的业务异常");
    }

    /**
     * 场景2：随机失败的消息
     * 模拟网络波动等随机故障场景
     */
    private void handleRandomFailure(String message, int reconsumeTimes) {
        double random = Math.random();
        logger.warn("【随机失败场景】随机值: {}, 当前重试次数: {}, 消息: {}", 
                random, reconsumeTimes, message);
        
        if (random > 0.7) { // 70%概率失败
            throw new RuntimeException("模拟随机失败: " + random);
        }
    }

    /**
     * 场景3：重试几次后成功的消息
     * 模拟临时故障恢复的场景
     */
    private void handleSuccessAfterRetry(String message, int reconsumeTimes) {
        // 解析期望的重试次数
        int expectedRetries = 2; // 默认重试2次后成功
        try {
            String[] parts = message.split("_");
            if (parts.length >= 4) {
                expectedRetries = Integer.parseInt(parts[3]);
            }
        } catch (Exception e) {
            // 使用默认值
        }
        
        logger.warn("【重试后成功场景】期望重试次数: {}, 当前重试次数: {}, 消息: {}", 
                expectedRetries, reconsumeTimes, message);
        
        if (reconsumeTimes < expectedRetries) {
            throw new RuntimeException("模拟临时故障，还需重试: " + (expectedRetries - reconsumeTimes) + "次");
        }
    }

    /**
     * 处理正常消息
     */
    private void handleNormalMessage(String message) {
        logger.info("【正常消费】处理消息: {}", message);
    }
}

