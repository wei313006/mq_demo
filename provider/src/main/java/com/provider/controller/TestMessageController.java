package com.provider.controller;

import com.provider.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class TestMessageController {

    private final MessageProducer messageProducer;

    @GetMapping("/test/rocketmq")
    public String sendNormalMessage(@RequestParam(defaultValue = "Hello RocketMQ") String message) {
        try {
            messageProducer.sendMessage(message);
            return "发送普通消息成功: " + message;
        } catch (Exception e) {
            log.error("发送普通消息失败, message: {}", message, e);
            return "发送失败: " + e.getMessage();
        }
    }

    @GetMapping("/test/delayed")
    public String sendDelayedMessage(@RequestParam(defaultValue = "Delayed Message") String message,
                                     @RequestParam(defaultValue = "5") int delaySeconds) {
        try {
            messageProducer.sendDelayMessage(message, delaySeconds);
            return "发送延迟消息成功: " + message + "，将在" + delaySeconds + "秒后消费";
        } catch (Exception e) {
            log.error("发送延迟消息失败, message: {}, delaySeconds: {}", message, delaySeconds, e);
            return "发送失败: " + e.getMessage();
        }
    }

    @GetMapping("/test/dlq")
    public String sendDlqMessage(@RequestParam(defaultValue = "Test DLQ") String message,
                                 @RequestParam(defaultValue = "测试死信") String reason) {
        try {
            messageProducer.sendDlqMessage(message, reason);
            return "发送死信消息成功: " + message;
        } catch (Exception e) {
            log.error("发送死信消息失败, message: {}, reason: {}", message, reason, e);
            return "发送失败: " + e.getMessage();
        }
    }

    @GetMapping("/test/retry/always-failure")
    public String sendAlwaysFailureMessage(@RequestParam(defaultValue = "总是失败测试") String message) {
        try {
            String sentMessage = messageProducer.sendAlwaysFailureMessage(message);
            return "【总是失败】消息已发送: " + sentMessage + "\n该消息会不断重试，最终进入死信队列（约10分钟后）";
        } catch (Exception e) {
            log.error("发送总是失败消息失败, message: {}", message, e);
            return "发送失败: " + e.getMessage();
        }
    }

    @GetMapping("/test/retry/random-failure")
    public String sendRandomFailureMessage(@RequestParam(defaultValue = "随机失败测试") String message) {
        try {
            String sentMessage = messageProducer.sendRandomFailureMessage(message);
            return "【随机失败】消息已发送: " + sentMessage + "\n70%概率失败，30%概率成功";
        } catch (Exception e) {
            log.error("发送随机失败消息失败, message: {}", message, e);
            return "发送失败: " + e.getMessage();
        }
    }

    @GetMapping("/test/retry/success-after")
    public String sendSuccessAfterRetryMessage(@RequestParam(defaultValue = "重试后成功测试") String message,
                                               @RequestParam(defaultValue = "2") int retryTimes) {
        try {
            String sentMessage = messageProducer.sendSuccessAfterRetryMessage(message, retryTimes);
            return "【重试后成功】消息已发送: " + sentMessage + "\n将在重试" + retryTimes + "次后成功";
        } catch (Exception e) {
            log.error("发送重试后成功消息失败, message: {}, retryTimes: {}", message, retryTimes, e);
            return "发送失败: " + e.getMessage();
        }
    }

    @GetMapping("/test/retry/normal")
    public String sendNormalRetryMessage(@RequestParam(defaultValue = "正常消息测试") String message) {
        try {
            String sentMessage = messageProducer.sendNormalRetryMessage(message);
            return "【正常消息】消息已发送: " + sentMessage + "\n应该立即消费成功";
        } catch (Exception e) {
            log.error("发送正常重试消息失败, message: {}", message, e);
            return "发送失败: " + e.getMessage();
        }
    }

    @GetMapping("/test/retry/all")
    public String sendAllTestMessages() {
        try {
            StringBuilder result = new StringBuilder();
            result.append("===== 综合测试开始 =====\n\n");

            result.append("1. 正常消息: ").append(messageProducer.sendNormalRetryMessage("综合测试-正常")).append("\n");
            result.append("2. 总是失败: ").append(messageProducer.sendAlwaysFailureMessage("综合测试-总是失败")).append("\n");
            result.append("3. 随机失败: ").append(messageProducer.sendRandomFailureMessage("综合测试-随机失败")).append("\n");
            result.append("4. 重试2次成功: ").append(messageProducer.sendSuccessAfterRetryMessage("综合测试-重试后成功", 2)).append("\n");

            result.append("\n===== 综合测试完成 =====");
            log.info("综合测试消息发送完成");
            return result.toString();
        } catch (Exception e) {
            log.error("发送综合测试消息失败", e);
            return "发送失败: " + e.getMessage();
        }
    }
}
