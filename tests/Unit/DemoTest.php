<?php

namespace hollisho\MQTransactionTests\Unit;

use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;

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
        $pdo = new \PDO('mysql:host=localhost;dbname=test', 'username', 'password');
        
        // 日志记录器
        $logger = new \Monolog\Logger('mq-transaction');
        $logger->pushHandler(new \Monolog\Handler\StreamHandler('path/to/your/app.log', \Monolog\Logger::INFO));
        
        // RabbitMQ连接配置
        $mqConfig = [
            'host' => 'localhost',
            'port' => 5672,
            'user' => 'guest',
            'password' => 'guest',
            'vhost' => '/',
            'exchanges' => [
                'order_exchange' => [
                    'type' => 'direct',
                    'durable' => true
                ]
            ],
            'queues' => [
                'order_created' => [
                    'durable' => true,
                    'binding' => [
                        'exchange' => 'order_exchange',
                        'routing_key' => 'order_created'
                    ]
                ]
            ]
        ];
        
        /*
         * 第二步：初始化框架组件
         */
        
        // 初始化事务助手
        $transactionHelper = new \hollisho\MQTransaction\Database\TransactionHelper(
            $pdo,
            'mq_messages'  // 消息表名
        );
        
        // 初始化幂等性助手
        $idempotencyHelper = new \hollisho\MQTransaction\Database\IdempotencyHelper(
            $pdo,
            'mq_idempotency'  // 幂等性表名
        );
        
        // 创建消息表和幂等性表（如果不存在）
        $transactionHelper->createTable();
        $idempotencyHelper->createTable();
        
        // 初始化MQ连接器
        $mqConnector = new \hollisho\MQTransaction\MQ\RabbitMQConnector($mqConfig, $logger);
        
        // 初始化服务容器（可选，用于依赖注入）
        $container = $this->createContainer();
        
        // 初始化补偿处理器
        $compensationHandler = new \hollisho\MQTransaction\Compensation\CompensationHandler($container, $logger);
        
        /*
         * 第三步：使用事务性消息生产者
         */
        
        // 创建事务性消息生产者
        $producer = new \hollisho\MQTransaction\Producer\TransactionProducer($mqConnector, $transactionHelper);
        
        // 开始事务
        $producer->beginTransaction();
        
        try {
            // 执行业务逻辑
            // ...
            // 例如，处理订单创建
            
            // 准备消息
            $producer->prepareMessage('order_created', [
                'order_id' => 1001,
                'user_id' => 2001,
                'amount' => 199.99,
                'status' => 'created',
                'create_time' => date('Y-m-d H:i:s')
            ]);
            
            // 提交事务（此时消息只是保存到本地消息表，尚未发送到MQ）
            $producer->commit();
        } catch (\Exception $e) {
            // 回滚事务
            $producer->rollback();
            throw $e;
        }
        
        /*
         * 第四步：使用消息分发器异步发送消息
         */
        
        // 创建消息分发器
        $dispatcher = new \hollisho\MQTransaction\Producer\MessageDispatcher($mqConnector, $transactionHelper, $logger);
        
        // 分发未发送的消息（通常在定时任务或后台进程中执行）
        $dispatcher->dispatch();
        
        /*
         * 第五步：使用消息消费者处理消息
         */
        
        // 创建消息消费者
        $consumer = new \hollisho\MQTransaction\Consumer\EventConsumer($mqConnector, $idempotencyHelper, $logger);
        
        // 注册消息处理器
        $consumer->registerHandler('order_created', function ($data) {
            // 处理订单创建消息
            // ...
            
            // 返回true表示处理成功，返回false表示处理失败需要重试
            return true;
        });
        
        // 启动消费（通常在独立的消费者进程中运行）
        $consumer->startConsuming(['order_created']);
        
        /*
         * 第六步：处理补偿逻辑
         */
        
        // 注册生产者失败的补偿处理器
        $compensationHandler->registerProducerHandler('order_created', function ($message) {
            // 处理消息发送失败的情况
            // 例如，撤销订单创建等操作
            // ...
            
            return true; // 返回true表示补偿处理成功
        });
        
        // 注册消费者失败的补偿处理器
        $compensationHandler->registerConsumerHandler('order_created', function ($message) {
            // 处理消息消费失败的情况
            // 例如，记录失败日志、发送告警等
            // ...
            
            return true; // 返回true表示补偿处理成功
        });
        
        // 手动触发生产者失败消息的补偿处理
        $compensationHandler->handleProducerFailure([
            'message_id' => 'failed-msg-1',
            'topic' => 'order_created',
            'data' => ['order_id' => 'ORDER-123', 'status' => 'failed']
        ]);
        
        // 手动触发消费者失败消息的补偿处理
        $compensationHandler->handleConsumerFailure([
            'message_id' => 'failed-consumption-1',
            'topic' => 'order_created',
            'data' => ['order_id' => 'ORDER-123', 'status' => 'processed']
        ]);
    }
    
    /**
     * 创建一个符合Psr\Container\ContainerInterface的容器实例
     *
     * @return ContainerInterface
     */
    private function createContainer()
    {
        $container = $this->createMock(ContainerInterface::class);
        // 在这里配置模拟容器的行为
        return $container;
    }
}