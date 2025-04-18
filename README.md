# PHP MQ Transaction

基于消息队列的PHP分布式事务管理组件，提供可靠的分布式事务解决方案。

## 特性

- **本地事务与消息发送的原子性**：确保本地事务与消息发送要么都成功，要么都失败
- **消息可靠投递**：通过异步消息发送机制确保消息最终被投递
- **消费幂等性**：防止消息重复消费导致的数据不一致
- **补偿机制**：当分布式事务出现异常时，支持自动触发补偿操作
- **多消息队列支持**：支持RabbitMQ和Kafka等主流消息队列

## 安装

```bash
composer require hollisho/php-mq-transaction
```

## 基本原理

本组件基于"可靠消息最终一致性"模式实现分布式事务，主要流程如下：

1. **预发送消息**：事务开始前，先将消息保存到本地消息表
2. **执行本地事务**：执行业务操作并提交本地事务
3. **异步投递消息**：后台进程定期扫描未成功发送的消息并投递到MQ
4. **消费者幂等处理**：消费者接收消息并幂等处理
5. **补偿机制**：当消息投递或消费失败时，触发补偿操作

## 使用示例

### 生产者示例

```php
// 创建事务性消息生产者
$producer = new TransactionProducer($mqConnector, $transactionHelper);

// 在事务中发送消息
$producer->beginTransaction();
try {
    // 执行本地业务逻辑
    $orderId = $orderService->createOrder($orderData);
    
    // 预发送消息
    $producer->prepareMessage('order_created', [
        'order_id' => $orderId,
        'order_amount' => $orderData['amount']
    ]);
    
    // 提交事务，消息会被标记为待发送
    $producer->commit();
} catch (\Exception $e) {
    // 回滚事务，消息也会被回滚
    $producer->rollback();
    throw $e;
}
```

### 消费者示例

```php
// 创建消息消费者
$consumer = new EventConsumer($mqConnector, $idempotencyHelper);

// 注册消息处理器
$consumer->registerHandler('order_created', function($message) use ($inventoryService) {
    // 幂等处理，防止重复消费
    $messageId = $message['message_id'];
    $data = $message['data'];
    
    // 执行业务逻辑
    $inventoryService->reduceStock($data['order_id'], $data['order_amount']);
    
    return true; // 返回true表示处理成功
});

// 启动消费
$consumer->start();
```

## 更多文档

请查看 `examples` 目录下的示例代码，了解更多使用方法。

## 许可证

MIT