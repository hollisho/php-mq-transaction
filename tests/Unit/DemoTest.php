<?php

namespace hollisho\MQTransactionTests\Unit;

use PHPUnit\Framework\TestCase;

/**
 * PHP-MQ-Transaction 框架使用演示
 *
 * 注意：这个文件仅用于演示框架的使用方法，不是实际可运行的测试代码。
 * 实际使用时请根据您的项目环境调整相关配置和代码。
 */
class DemoTest extends TestCase
{
    /**
     * 演示：基本使用流程
     */
    public function testExample()
    {
        // 由于这只是演示，我们跳过实际测试
        $this->markTestSkipped('这是一个演示方法，仅用于展示框架使用方式');
        
        // 以下代码展示了 php-mq-transaction 框架的使用方式
        
        /*
         * 第一步：初始化基础组件
         */
        
        // 数据库连接
        $pdo = new \PDO('mysql:host=localhost;dbname=your_database', 'username', 'password');
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        
        // 日志记录器 (使用任何兼容 PSR-3 的日志实现)
        $logger = new \Monolog\Logger('mq-transaction');
        $logger->pushHandler(new \Monolog\Handler\StreamHandler('logs/app.log', \Monolog\Logger::DEBUG));
        
        // 初始化事务助手
        $transactionHelper = new \hollisho\MQTransaction\Database\TransactionHelper($pdo);
        
        // 初始化幂等性助手
        $idempotencyHelper = new \hollisho\MQTransaction\Database\IdempotencyHelper($pdo);
        
        // 如果表不存在，创建消息表和幂等性表
        $transactionHelper->createTable();
        $idempotencyHelper->createTable();
        
        // RabbitMQ 连接配置
        $mqConfig = [
            'host' => 'localhost',
            'port' => 5672,
            'user' => 'guest',
            'password' => 'guest',
            'vhost' => '/'
        ];
        
        // 初始化 RabbitMQ 连接器
        $mqConnector = new \hollisho\MQTransaction\MQ\RabbitMQConnector($mqConfig, $logger);
        
        /*
         * 第二步：事务性消息发送示例
         */
        
        // 创建事务性消息生产者
        $producer = new \hollisho\MQTransaction\Producer\TransactionProducer($mqConnector, $transactionHelper);
        
        try {
            // 开始事务
            $producer->beginTransaction();
            
            // 执行本地业务操作
            $stmt = $pdo->prepare("INSERT INTO orders (order_id, user_id, amount) VALUES (?, ?, ?)");
            $orderId = uniqid('order-');
            $userId = 1001;
            $amount = 199.99;
            $stmt->execute([$orderId, $userId, $amount]);
            
            // 准备消息
            $orderCreatedMessage = [
                'order_id' => $orderId,
                'user_id' => $userId,
                'amount' => $amount,
                'status' => 'created',
                'create_time' => date('Y-m-d H:i:s')
            ];
            
            // 将消息与事务关联
            $messageId = $producer->prepareMessage(
                'order.created',      // 消息主题
                $orderCreatedMessage, // 消息数据
                ['delivery_mode' => 2] // 消息选项
            );
            
            // 提交事务 - 这将同时提交数据库事务和保存消息
            $producer->commit();
            
            echo "订单创建成功，消息ID: $messageId\n";
            
        } catch (\Exception $e) {
            // 回滚事务
            $producer->rollback();
            echo "订单创建失败: " . $e->getMessage() . "\n";
        }
        
        /*
         * 第三步：消息分发 (可以在独立进程或定时任务中运行)
         */
        
        // 创建消息分发器
        $dispatcher = new \hollisho\MQTransaction\Producer\MessageDispatcher($mqConnector, $transactionHelper, $logger);
        
        // 设置批处理大小和最大重试次数
        $dispatcher->setBatchSize(100)
                  ->setMaxRetryCount(5);
        
        // 执行一次分发
        $sentCount = $dispatcher->dispatch();
        echo "成功发送 $sentCount 条消息\n";
        
        // 或者作为长期运行的服务
        // $dispatcher->run(5);  // 每5秒轮询一次
        
        /*
         * 第四步：消息消费
         */
        
        // 创建事件消费者
        $consumer = new \hollisho\MQTransaction\Consumer\EventConsumer(
            $mqConnector, 
            $idempotencyHelper,
            $logger
        );
        
        // 注册消息处理器
        $consumer->registerHandler('order.created', function($message) use ($logger) {
            $logger->info('处理订单创建消息', ['order_id' => $message['order_id']]);
            
            // 处理业务逻辑，例如发送邮件通知、更新库存等
            
            // 返回true表示处理成功，消息将被确认
            return true;
        });
        
        // 开始消费消息
        try {
            // 在实际应用中，这个调用会阻塞并持续消费消息
            // $consumer->startConsuming(['order.created']);
            
            // 在测试中可以手动处理消息
            $testMessage = [
                'message_id' => 'test-id',
                'topic' => 'order.created',
                'data' => ['order_id' => $orderId, 'amount' => $amount],
                'raw_message' => (object)['body' => '{}']
            ];
            $consumer->processMessage($testMessage);
            
        } catch (\Exception $e) {
            echo "消息消费异常: " . $e->getMessage() . "\n";
        }
        
        /*
         * 第五步：补偿处理 (处理失败消息)
         */
        
        // 创建补偿处理器
        // 注意：在实际项目中，您需要提供一个实现 PSR-11 ContainerInterface 的容器
        // 这里仅作为示例，实际使用时请替换为您的容器实现
        $compensationHandler = new \hollisho\MQTransaction\Compensation\CompensationHandler(null, $logger);
        
        // 注册生产者补偿处理器
        $compensationHandler->registerProducerHandler('order.created', function($message) use ($logger) {
            $logger->info('执行订单创建补偿', ['message_id' => $message['message_id']]);
            // 执行补偿逻辑，例如取消订单、退款等
            return true;
        });
        
        // 注册消费者补偿处理器
        $compensationHandler->registerConsumerHandler('order.created', function($consumption) use ($logger) {
            $logger->info('执行订单处理补偿', ['message_id' => $consumption['message_id']]);
            // 执行补偿逻辑
            return true;
        });
        
        // 手动触发补偿处理 - 生产者失败消息
        $failedMessage = [
            'message_id' => 'failed-msg-1',
            'topic' => 'order.created',
            'data' => ['order_id' => 'ORDER-123', 'status' => 'failed']
        ];
        
        $result = $compensationHandler->handleProducerFailure($failedMessage);
        echo "生产者补偿处理结果: " . ($result ? "成功" : "失败") . "\n";
        
        // 手动触发补偿处理 - 消费者失败记录
        $failedConsumption = [
            'message_id' => 'failed-consumption-1',
            'topic' => 'order.created',
            'data' => ['order_id' => 'ORDER-123', 'status' => 'processed']
        ];
        
        $result = $compensationHandler->handleConsumerFailure($failedConsumption);
        echo "消费者补偿处理结果: " . ($result ? "成功" : "失败") . "\n";
        
        // 演示成功
        $this->assertTrue(true);
    }
}