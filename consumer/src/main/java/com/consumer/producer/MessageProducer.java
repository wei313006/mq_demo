
package com.consumer.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class MessageProducer {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Value("${rocketmq.name-server:192.168.227.145:9876}")
    private String nameServer;

    private DefaultMQProducer defaultMQProducer;

    private static final String TOPIC_NORMAL = "test-topic";
    private static final String TOPIC_DELAY = "test-delay-topic";
    private static final String TOPIC_DLQ = "test-dlq-topic";
    private static final String TOPIC_RETRY = "test-retry-topic";

    // 消息前缀定义
    private static final String FAILURE_PREFIX = "FAILURE_";
    private static final String RANDOM_FAILURE_PREFIX = "RANDOM_FAILURE_";
    private static final String SUCCESS_AFTER_RETRY_PREFIX = "SUCCESS_AFTER_RETRY_";

    @PostConstruct
    public void init() {
        defaultMQProducer = new DefaultMQProducer("test-producer-group");
        defaultMQProducer.setNamesrvAddr(nameServer);
        try {
            defaultMQProducer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String message) {
        rocketMQTemplate.convertAndSend(TOPIC_NORMAL, message);
    }

    public void sendDelayMessage(String message, int delaySeconds) {
        try {
            int delayLevel = getDelayLevel(delaySeconds);
            Message msg = new Message(TOPIC_DELAY, message.getBytes());
            msg.setDelayTimeLevel(delayLevel);
            SendResult sendResult = defaultMQProducer.send(msg);
            System.out.println("发送延迟消息成功，延迟级别：" + delayLevel + "，消息ID：" + sendResult.getMsgId());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendDlqMessage(String message, String reason) {
        String dlqMessage = String.format("Reason: %s | Original: %s", reason, message);
        rocketMQTemplate.convertAndSend(TOPIC_DLQ, dlqMessage);
    }

    /**
     * 发送总是失败的消息
     * 这种消息会不断重试，最终进入死信队列
     */
    public String sendAlwaysFailureMessage(String message) {
        String fullMessage = FAILURE_PREFIX + message;
        rocketMQTemplate.convertAndSend(TOPIC_RETRY, fullMessage);
        return fullMessage;
    }

    /**
     * 发送随机失败的消息
     * 模拟网络波动等随机故障场景
     */
    public String sendRandomFailureMessage(String message) {
        String fullMessage = RANDOM_FAILURE_PREFIX + message;
        rocketMQTemplate.convertAndSend(TOPIC_RETRY, fullMessage);
        return fullMessage;
    }

    /**
     * 发送重试指定次数后成功的消息
     * 模拟临时故障恢复的场景
     */
    public String sendSuccessAfterRetryMessage(String message, int retryTimes) {
        String fullMessage = SUCCESS_AFTER_RETRY_PREFIX + retryTimes + "_" + message;
        rocketMQTemplate.convertAndSend(TOPIC_RETRY, fullMessage);
        return fullMessage;
    }

    /**
     * 发送正常消息到重试topic
     */
    public String sendNormalRetryMessage(String message) {
        rocketMQTemplate.convertAndSend(TOPIC_RETRY, message);
        return message;
    }

    private int getDelayLevel(int delaySeconds) {
        int[] delayTimes = {1, 5, 10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};
        for (int i = 0; i < delayTimes.length; i++) {
            if (delaySeconds <= delayTimes[i]) {
                return i + 1;
            }
        }
        return delayTimes.length;
    }

    @PreDestroy
    public void shutdown() {
        if (defaultMQProducer != null) {
            defaultMQProducer.shutdown();
        }
    }
}
