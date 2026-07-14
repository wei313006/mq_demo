
# RocketMQ 延迟、死信队列

## 概述
本示例演示了rocket mq延迟队列和死信队列机制。

## 核心概念

### 1. 重试机制
- RocketMQ默认重试16次
- 重试间隔递增：1s, 5s, 10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m
- 总计约4小时46分钟

### 2. 死信队列（Dead Letter Queue）
- 消息重试超过最大次数后，会自动进入死信队列
- 死信Topic格式：`%DLQ%{consumerGroup}`
- 需要单独的消费者来处理死信消息

## 项目结构

### 生产者（Provider模块，端口 8060）
- `MessageProducer.java` - 消息发送器，支持发送各种测试场景的消息
- `TestMessageController.java` - REST接口，提供消息发送测试端点
- 同时提供 Dubbo 服务实现

### 消费者（Consumer模块，端口 8061）
- `MessageConsumer.java` - 普通消息消费者
- `DelayedMessageConsumer.java` - 延迟消息消费者
- `RetryMessageConsumer.java` - 带有重试机制的消费者
- `DeadLetterQueueConsumer.java` - 死信队列消费者
- `EnhancedDeadLetterQueueConsumer.java` - 增强版死信队列消费者
- 同时作为 Dubbo 消费方

## 测试接口说明

所有消息发送接口位于 **Provider 模块（端口 8060）**

### 1. 总是失败的消息
```
GET http://localhost:8060/test/retry/always-failure?message=测试消息
```
- 描述：消息会不断失败，最终进入死信队列
- 预期：约10分钟后进入死信队列（达到第16次重试）

### 2. 随机失败的消息
```
GET http://localhost:8060/test/retry/random-failure?message=测试消息
```
- 描述：70%概率失败，30%概率成功
- 模拟网络波动场景

### 3. 重试后成功的消息
```
GET http://localhost:8060/test/retry/success-after?message=测试消息&retryTimes=2
```
- 描述：重试指定次数后成功
- 参数：retryTimes指定重试次数

### 4. 正常消息
```
GET http://localhost:8060/test/retry/normal?message=测试消息
```
- 描述：应该立即消费成功

### 5. 延迟队列
```
GET http://localhost:8060/test/delayed?message=测试消息&delaySeconds=3
```
- 描述：三秒后消息发送，Consumer模块控制台打印：收到延迟消息:xxx

### 6. 普通消息
```
GET http://localhost:8060/test/rocketmq?message=测试消息
```
- 描述：发送普通消息到 test-topic

## 观察日志

所有消费日志在 **Consumer 模块（端口 8061）** 控制台查看。

### 重试过程
观察 Consumer 模块控制台：
```
========== 收到消息 ==========
消息ID: xxx, 重试次数: 0, 消息内容: FAILURE_xxx, 接收时间: ...
【总是失败场景】当前重试次数: 0, 消息: FAILURE_xxx
消息消费失败，触发重试机制
```

### 死信队列
观察 Consumer 模块控制台：
```
========================================
【死信队列告警】收到死信消息
========================================
消息ID: xxx
原始Topic: test-retry-topic
死信Topic: %DLQ%test-retry-consumer-group
重试次数: 16
...
```

## 生产环境建议

### 1. 消息幂等性
- 所有消费者必须保证幂等性
- 使用消息ID或业务唯一键作为幂等键

### 2. 死信消息处理
- 死信消息必须被关注和处理
- 建议存入数据库，支持人工重试
- 发送告警通知（钉钉、企业微信等）

### 3. 监控告警
- 监控死信队列消息数量
- 监控消费失败率
- 设置合理的告警阈值

### 4. 消息追踪
- 记录消息发送、消费、重试、死信的完整链路
- 保存消息上下文信息

## 配置说明（ip自行修改）

### application.yml
```yaml
rocketmq:
  name-server: 192.168.227.145:9876
```

### 消费者组配置
在 `@RocketMQMessageListener` 注解中配置：
- `topic`: 订阅的Topic
- `consumerGroup`: 消费者组
- `consumeMode`: 消费模式（ORDERLY/CONCURRENTLY）

## 常见问题

###  如何调整重试次数？
RocketMQ不支持直接配置重试次数。业务逻辑中可以根据reconsumeTimes判断是否继续重试或直接丢弃。

### 如何手动处理死信消息？
可以通过RocketMQ控制台或API重新发送死信消息，也可以在死信消费者中实现自动重发逻辑。

### 死信消息会丢失吗？
不会，除非显式删除。RocketMQ会持久化死信消息。
