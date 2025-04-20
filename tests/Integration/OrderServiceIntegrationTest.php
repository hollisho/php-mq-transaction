<?php

namespace hollisho\MQTransactionTests\Integration;

use hollisho\MQTransaction\Database\TransactionHelper;
use hollisho\MQTransaction\MQ\RabbitMQConnector;
use hollisho\MQTransaction\Producer\TransactionProducer;
use hollisho\MQTransactionTests\Examples\OrderService\OrderService;
use PHPUnit\Framework\TestCase;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

/**
 * 事务监控类，用于捕获事务操作
 */
class MonitoredTransactionHelper extends \hollisho\MQTransaction\Database\TransactionHelper
{
    private $logger;
    
    public function __construct(\PDO $pdo, string $tableName = 'mq_messages', \Psr\Log\LoggerInterface $logger = null)
    {
        parent::__construct($pdo, $tableName, true);
        $this->logger = $logger;
    }
    
    public function beginTransaction(): bool
    {
        if ($this->logger) {
            $backtrace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 3);
            $caller = isset($backtrace[1]) ? $backtrace[1]['class'] . '::' . $backtrace[1]['function'] : 'unknown';
            $this->logger->info("========= 事务开始 =========", [
                'caller' => $caller,
                'time' => date('Y-m-d H:i:s')
            ]);
        }
        
        return parent::beginTransaction();
    }
    
    public function commit(): bool
    {
        if ($this->logger) {
            $backtrace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 3);
            $caller = isset($backtrace[1]) ? $backtrace[1]['class'] . '::' . $backtrace[1]['function'] : 'unknown';
            $this->logger->info("========= 事务提交 =========", [
                'caller' => $caller,
                'time' => date('Y-m-d H:i:s')
            ]);
        }
        
        try {
            $result = parent::commit();
            
            if ($this->logger) {
                $this->logger->info("事务提交结果", ['result' => $result ? 'success' : 'failed']);
            }
            
            return $result;
        } catch (\Exception $e) {
            if ($this->logger) {
                $this->logger->error("事务提交异常", [
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
            }
            throw $e;
        }
    }
    
    public function rollback(): bool
    {
        if ($this->logger) {
            $backtrace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 3);
            $caller = isset($backtrace[1]) ? $backtrace[1]['class'] . '::' . $backtrace[1]['function'] : 'unknown';
            $this->logger->info("========= 事务回滚 =========", [
                'caller' => $caller,
                'time' => date('Y-m-d H:i:s')
            ]);
        }
        
        try {
            $result = parent::rollback();
            
            if ($this->logger) {
                $this->logger->info("事务回滚结果", ['result' => $result ? 'success' : 'failed']);
            }
            
            return $result;
        } catch (\Exception $e) {
            if ($this->logger) {
                $this->logger->error("事务回滚异常", [
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
            }
            throw $e;
        }
    }
    
    public function saveMessage(array $message): bool
    {
        if ($this->logger) {
            $this->logger->info("保存消息", [
                'message_id' => $message['message_id'],
                'topic' => $message['topic'],
                'status' => $message['status']
            ]);
        }
        
        try {
            $result = parent::saveMessage($message);
            
            if ($this->logger) {
                $this->logger->info("保存消息结果", ['result' => $result ? 'success' : 'failed']);
            }
            
            return $result;
        } catch (\Exception $e) {
            if ($this->logger) {
                $this->logger->error("保存消息异常", [
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
            }
            throw $e;
        }
    }
}

/**
 * OrderService 集成测试
 * 
 * 注意：这个测试使用真实的数据库和消息队列连接
 * 需要在测试前正确配置数据库和RabbitMQ
 */
class OrderServiceIntegrationTest extends TestCase
{
    /**
     * @var \PDO 数据库连接
     */
    private $pdo;
    
    /**
     * @var TransactionHelper 事务助手
     */
    private $transactionHelper;
    
    /**
     * @var RabbitMQConnector 消息队列连接器
     */
    private $mqConnector;
    
    /**
     * @var TransactionProducer 事务生产者
     */
    private $producer;
    
    /**
     * @var OrderService 订单服务
     */
    private $orderService;
    
    /**
     * @var Logger 日志记录器
     */
    private $logger;
    
    /**
     * 测试前准备工作
     */
    protected function setUp(): void
    {
        // 跳过测试，除非明确指定要运行集成测试
        if (!getenv('RUN_INTEGRATION_TESTS')) {
            $this->markTestSkipped('集成测试已跳过。设置环境变量 RUN_INTEGRATION_TESTS=1 来运行这些测试。');
        }
        
        // 创建日志记录器
        $this->logger = new Logger('test');
        $this->logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));
        
        // 连接数据库
        $this->pdo = new \PDO(
            getenv('DB_DSN') ?: 'mysql:host=localhost;dbname=test;charset=utf8mb4',
            getenv('DB_USER') ?: 'root',
            getenv('DB_PASSWORD') ?: '',
            [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC
            ]
        );
        
        // 检查和设置事务隔离级别
        try {
            $currentIsolation = $this->pdo->query("SELECT @@transaction_isolation")->fetchColumn();
            $this->logger->info("当前事务隔离级别", ['level' => $currentIsolation]);
            
            // 设置为可重复读，确保事务正确处理
            $this->pdo->exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            $newIsolation = $this->pdo->query("SELECT @@transaction_isolation")->fetchColumn();
            $this->logger->info("设置后事务隔离级别", ['level' => $newIsolation]);
            
            // 确保自动提交关闭 - 使用MySQL特定的方式设置
            $this->pdo->exec("SET autocommit=0");
            $autoCommit = $this->pdo->query("SELECT @@autocommit")->fetchColumn();
            $this->logger->info("MySQL自动提交设置", ['autocommit' => $autoCommit]);
            
            // 也使用PDO API设置，以防某些驱动需要
            $this->pdo->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
            $pdoAutoCommit = $this->pdo->getAttribute(\PDO::ATTR_AUTOCOMMIT);
            $this->logger->info("PDO自动提交设置", ['autocommit' => $pdoAutoCommit]);
        } catch (\Exception $e) {
            $this->logger->warning("设置事务隔离级别失败", ['error' => $e->getMessage()]);
        }
        
        // 创建事务助手
        $this->transactionHelper = new MonitoredTransactionHelper($this->pdo, 'mq_messages', $this->logger);
        
        // 确保消息表存在
        $this->transactionHelper->createTable();
        
        // 创建订单相关的表
        $this->createOrderTables();
        
        // 创建RabbitMQ连接
        $mqConfig = [
            'host' => getenv('MQ_HOST') ?: 'localhost',
            'port' => (int)(getenv('MQ_PORT') ?: 5672),
            'user' => getenv('MQ_USER') ?: 'guest',
            'password' => getenv('MQ_PASSWORD') ?: 'guest',
            'vhost' => getenv('MQ_VHOST') ?: '/'
        ];
        $this->mqConnector = new RabbitMQConnector($mqConfig, $this->logger);
        
        // 初始化RabbitMQ交换机和队列
        $this->setupRabbitMQ();
        
        // 创建事务生产者
        $this->producer = new TransactionProducer(
            $this->mqConnector,
            $this->transactionHelper
        );
        
        // 创建订单服务
        $this->orderService = new OrderService($this->pdo, $this->producer);
        
        // 测试数据库连接是否正常工作
        try {
            $this->pdo->exec("INSERT INTO orders (user_id, total_amount, status, created_at) 
                             VALUES (999, 9.99, 'pending', NOW())");
            $testOrderId = $this->pdo->lastInsertId();
            $this->logger->info("测试数据库连接成功，创建了测试订单", ['test_order_id' => $testOrderId]);
        } catch (\Exception $e) {
            $this->logger->error("测试数据库连接失败", ['error' => $e->getMessage()]);
        }
    }
    
    /**
     * 设置RabbitMQ交换机和队列
     */
    private function setupRabbitMQ(): void
    {
        // 检查数据库连接是否有效
        try {
            // 检查mq_messages表是否存在
            $stmt = $this->pdo->query("SHOW TABLES LIKE 'mq_messages'");
            $tableExists = $stmt->fetch();
            $this->logger->info("mq_messages表状态", ['exists' => $tableExists ? 'yes' : 'no']);
            
            if (!$tableExists) {
                $this->logger->info("创建mq_messages表");
                $this->transactionHelper->createTable();
            }
            
            // 检查PDO属性
            $this->logger->info("PDO属性", [
                'ERRMODE' => $this->pdo->getAttribute(\PDO::ATTR_ERRMODE),
                'AUTOCOMMIT' => $this->pdo->getAttribute(\PDO::ATTR_AUTOCOMMIT)
            ]);
        } catch (\Exception $e) {
            $this->logger->error("数据库连接检查失败", ['error' => $e->getMessage()]);
        }
        
        // 定义需要创建的主题
        $topics = ['order_created', 'order_cancelled', 'order_paid'];
        
        // 创建交换机
        $exchangeName = 'order_exchange';
        $this->mqConnector->declareExchange($exchangeName, 'topic', true, false);
        
        // 创建队列并绑定
        foreach ($topics as $topic) {
            $queueName = $topic . '_queue';
            
            // 声明队列
            $this->mqConnector->declareQueue($queueName, true, false, false);
            
            // 绑定队列到交换机
            $this->mqConnector->bindQueue($queueName, $exchangeName, $topic);
            
            $this->logger->info("创建队列和绑定", [
                'queue' => $queueName,
                'exchange' => $exchangeName,
                'routing_key' => $topic
            ]);
        }
        
        // 注意：如果RabbitMQConnector没有提供setDefaultExchange方法，
        // 需要修改OrderService或确保它使用正确的交换机名称
    }
    
    /**
     * 测试结束后清理
     */
    protected function tearDown(): void
    {
        if (!getenv('RUN_INTEGRATION_TESTS')) {
            return;
        }
        
        $this->logger->info("===== 测试结束，开始清理 =====");
        
        try {
            // 检查是否有未完成的事务
            if ($this->pdo->inTransaction()) {
                $this->logger->warning("发现未完成的事务，尝试回滚");
                try {
                    $this->pdo->rollBack();
                } catch (\Exception $e) {
                    $this->logger->error("回滚未完成事务失败", ['error' => $e->getMessage()]);
                }
            }
            
            // 确保消息分发
            $this->dispatchPendingMessages();
        } catch (\Exception $e) {
            $this->logger->error("消息分发失败", ['error' => $e->getMessage()]);
        }
        
        try {
            // 检查并设置自动提交
            $this->pdo->exec("SET autocommit=1");
            $this->pdo->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            $autoCommit = $this->pdo->query("SELECT @@autocommit")->fetchColumn();
            $this->logger->info("自动提交设置为开启", ['autocommit' => $autoCommit]);
            
            // 清理测试数据
            $this->logger->info("清理测试数据");
            $this->pdo->exec("DELETE FROM order_items");
            $this->pdo->exec("DELETE FROM order_payments");
            $this->pdo->exec("DELETE FROM orders");
            $this->pdo->exec("DELETE FROM mq_messages");
        } catch (\Exception $e) {
            $this->logger->error("清理测试数据失败", ['error' => $e->getMessage()]);
        }
        
        try {
            // 关闭连接
            $this->logger->info("关闭连接");
            $this->mqConnector->close();
            $this->pdo = null;
        } catch (\Exception $e) {
            $this->logger->error("关闭连接失败", ['error' => $e->getMessage()]);
        }
        
        $this->logger->info("===== 清理完成 =====");
    }
    
    /**
     * 分发所有待处理的消息
     */
    private function dispatchPendingMessages(): void
    {
        $this->logger->info("开始分发待处理消息");
        
        // 确保没有未完成的事务
        if ($this->pdo->inTransaction()) {
            $this->logger->warning("发现未完成的事务，尝试提交");
            try {
                $this->pdo->commit();
            } catch (\Exception $e) {
                $this->logger->error("提交未完成事务失败", ['error' => $e->getMessage()]);
                try {
                    $this->pdo->rollBack();
                } catch (\Exception $e2) {
                    $this->logger->error("回滚未完成事务也失败", ['error' => $e2->getMessage()]);
                }
            }
        }
        
        // 获取待处理消息
        $stmt = $this->pdo->prepare("SELECT * FROM mq_messages WHERE status = 'pending'");
        $stmt->execute();
        $messages = $stmt->fetchAll();
        
        if (empty($messages)) {
            $this->logger->info("没有待处理消息需要分发");
            
            // 查看消息表中所有记录
            $stmt = $this->pdo->query("SELECT * FROM mq_messages");
            $allMessages = $stmt->fetchAll();
            
            if (empty($allMessages)) {
                $this->logger->warning("消息表中没有任何记录");
            } else {
                $this->logger->info("消息表中有 " . count($allMessages) . " 条记录，但没有待处理的消息");
                foreach ($allMessages as $msg) {
                    $this->logger->info("消息记录", [
                        'id' => $msg['id'], 
                        'message_id' => $msg['message_id'], 
                        'topic' => $msg['topic'], 
                        'status' => $msg['status']
                    ]);
                }
            }
            
            return;
        }
        
        $this->logger->info("找到 " . count($messages) . " 条待处理消息");
        
        // 模拟分发消息
        foreach ($messages as $message) {
            $this->logger->info("分发消息", [
                'id' => $message['id'],
                'message_id' => $message['message_id'],
                'topic' => $message['topic'],
                'data' => substr($message['data'], 0, 100) . '...'
            ]);
            
            try {
                // 连接到RabbitMQ并发送消息
                $exchangeName = 'order_exchange';
                $result = $this->mqConnector->send(
                    $message['topic'],
                    json_decode($message['data'], true),
                    $message['message_id'],
                    ['exchange' => $exchangeName, 'delivery_mode' => 2]  // 持久化消息
                );
                
                if (!$result) {
                    $this->logger->error("消息发送失败", ['id' => $message['id']]);
                    continue;
                }
                
                // 更新消息状态为已分发 - 使用带重试的方式
                $maxRetries = 3;
                $retryCount = 0;
                $updateSuccess = false;
                
                while ($retryCount < $maxRetries && !$updateSuccess) {
                    try {
                        // 使用较短的单独事务来更新状态
                        $this->pdo->beginTransaction();
                        $updateStmt = $this->pdo->prepare("UPDATE mq_messages SET status = 'sent', updated_at = NOW() WHERE id = :id");
                        $updateResult = $updateStmt->execute(['id' => $message['id']]);
                        $this->pdo->commit();
                        
                        $updateSuccess = true;
                        $this->logger->info("消息分发成功，状态已更新", ['id' => $message['id']]);
                    } catch (\PDOException $e) {
                        if ($this->pdo->inTransaction()) {
                            $this->pdo->rollBack();
                        }
                        
                        // 检查是否是锁等待超时
                        if (strpos($e->getMessage(), 'Lock wait timeout') !== false) {
                            $retryCount++;
                            $this->logger->warning("锁等待超时，重试 ({$retryCount}/{$maxRetries})", ['id' => $message['id']]);
                            sleep(1); // 等待一秒再重试
                        } else {
                            // 其他PDO错误
                            $this->logger->error("更新消息状态失败", [
                                'id' => $message['id'], 
                                'error' => $e->getMessage()
                            ]);
                            break;
                        }
                    }
                }
                
                if (!$updateSuccess) {
                    $this->logger->error("多次尝试后仍无法更新消息状态", ['id' => $message['id']]);
                }
            } catch (\Exception $e) {
                $this->logger->error("消息分发失败", [
                    'id' => $message['id'],
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
                
                try {
                    // 重试更新失败计数
                    $this->pdo->beginTransaction();
                    $updateStmt = $this->pdo->prepare("UPDATE mq_messages SET retry_count = retry_count + 1 WHERE id = :id");
                    $updateStmt->execute(['id' => $message['id']]);
                    $this->pdo->commit();
                } catch (\Exception $e2) {
                    if ($this->pdo->inTransaction()) {
                        $this->pdo->rollBack();
                    }
                    $this->logger->error("更新失败计数也失败", ['id' => $message['id'], 'error' => $e2->getMessage()]);
                }
            }
        }
        
        $this->logger->info("消息分发完成");
    }
    
    /**
     * 创建订单相关的表结构
     */
    private function createOrderTables(): void
    {
        // 创建订单表
        $this->pdo->exec("CREATE TABLE IF NOT EXISTS orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            status ENUM('pending', 'paid', 'shipped', 'cancelled') NOT NULL DEFAULT 'pending',
            created_at DATETIME NOT NULL,
            updated_at DATETIME DEFAULT NULL
        )");
        
        // 创建订单项表
        $this->pdo->exec("CREATE TABLE IF NOT EXISTS order_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
        )");
        
        // 创建支付记录表
        $this->pdo->exec("CREATE TABLE IF NOT EXISTS order_payments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT NOT NULL,
            payment_method VARCHAR(50) NOT NULL,
            transaction_id VARCHAR(100) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status ENUM('pending', 'success', 'failed') NOT NULL,
            created_at DATETIME NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
        )");
    }
    
    /**
     * 测试订单创建流程
     */
    public function testCreateOrder()
    {
        // 准备测试数据
        $orderData = [
            'user_id' => 1001,
            'total_amount' => 199.99,
            'items' => [
                [
                    'product_id' => 101,
                    'quantity' => 2,
                    'price' => 99.99
                ]
            ]
        ];
        
        // 输出当前数据库状态
        $this->logger->info("===== 测试前数据库状态 =====");
        
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM orders");
        $orderCount = $stmt->fetchColumn();
        $this->logger->info("订单表记录数", ['count' => $orderCount]);
        
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM mq_messages");
        $messageCount = $stmt->fetchColumn();
        $this->logger->info("消息表记录数", ['count' => $messageCount]);
        
        // 执行订单创建
        try {
            $orderId = $this->orderService->createOrder($orderData);
            $this->logger->info("订单创建成功", ['order_id' => $orderId]);
        } catch (\Exception $e) {
            $this->logger->error("订单创建失败", ['error' => $e->getMessage(), 'trace' => $e->getTraceAsString()]);
            throw $e;
        }
        
        // 输出创建后数据库状态
        $this->logger->info("===== 创建后数据库状态 =====");
        
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM orders");
        $orderCount = $stmt->fetchColumn();
        $this->logger->info("订单表记录数", ['count' => $orderCount]);
        
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM mq_messages");
        $messageCount = $stmt->fetchColumn();
        $this->logger->info("消息表记录数", ['count' => $messageCount]);
        
        // 检查最新创建的订单
        $stmt = $this->pdo->prepare("SELECT * FROM orders ORDER BY id DESC LIMIT 1");
        $stmt->execute();
        $latestOrder = $stmt->fetch();
        $this->logger->info("最新订单信息", ['order' => $latestOrder ? json_encode($latestOrder) : 'none']);
        
        // 立即分发消息以确保消息发送到RabbitMQ
        $this->dispatchPendingMessages();
        
        // 验证订单ID是否有效
        $this->assertNotEmpty($orderId, '订单ID不应为空');
        $this->assertIsNumeric($orderId, '订单ID应该是数字');
        
        // 从数据库验证订单数据
        $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = ?");
        $stmt->execute([$orderId]);
        $order = $stmt->fetch();
        
        $this->assertNotEmpty($order, '订单数据应该存在于数据库中');
        $this->assertEquals($orderData['user_id'], $order['user_id'], '用户ID应匹配');
        $this->assertEquals($orderData['total_amount'], $order['total_amount'], '订单金额应匹配');
        $this->assertEquals('pending', $order['status'], '初始状态应为pending');
        
        // 验证订单项
        $stmt = $this->pdo->prepare("SELECT * FROM order_items WHERE order_id = ?");
        $stmt->execute([$orderId]);
        $items = $stmt->fetchAll();
        
        $this->assertCount(count($orderData['items']), $items, '订单项数量应匹配');
        $this->assertEquals($orderData['items'][0]['product_id'], $items[0]['product_id'], '商品ID应匹配');
        $this->assertEquals($orderData['items'][0]['quantity'], $items[0]['quantity'], '数量应匹配');
        $this->assertEquals($orderData['items'][0]['price'], $items[0]['price'], '价格应匹配');
        
        // 验证消息是否已保存到消息表
        $stmt = $this->pdo->prepare("SELECT * FROM mq_messages WHERE topic = 'order_created'");
        $stmt->execute();
        $messages = $stmt->fetchAll();
        
        $this->assertNotEmpty($messages, '应该有订单创建消息');
        $messageData = json_decode($messages[0]['data'], true);
        $this->assertEquals($orderId, $messageData['order_id'], '消息中的订单ID应匹配');
        $this->assertEquals($orderData['user_id'], $messageData['user_id'], '消息中的用户ID应匹配');
        
        // 使用消息验证方法再次确认
        $messageValid = $this->validateMessages('order_created', function($data) use ($orderId, $orderData) {
            return isset($data['order_id']) && $data['order_id'] == $orderId &&
                   isset($data['user_id']) && $data['user_id'] == $orderData['user_id'];
        });
        $this->assertTrue($messageValid, '订单创建消息验证应通过');
        
        $this->logger->info("创建订单测试成功", ['order_id' => $orderId]);
    }
    
    /**
     * 测试订单取消流程
     */
    public function testCancelOrder()
    {
        // 先创建一个订单
        $orderData = [
            'user_id' => 1002,
            'total_amount' => 299.99,
            'items' => [
                [
                    'product_id' => 102,
                    'quantity' => 1,
                    'price' => 299.99
                ]
            ]
        ];
        
        $orderId = $this->orderService->createOrder($orderData);
        $this->assertNotEmpty($orderId, '订单创建应成功');
        
        // 分发创建订单的消息
        $this->dispatchPendingMessages();
        
        // 执行取消操作
        $result = $this->orderService->cancelOrder($orderId);
        
        // 分发取消订单的消息
        $this->dispatchPendingMessages();
        
        // 验证取消结果
        $this->assertTrue($result, '取消订单应返回true');
        
        // 从数据库验证订单状态
        $stmt = $this->pdo->prepare("SELECT status FROM orders WHERE id = ?");
        $stmt->execute([$orderId]);
        $status = $stmt->fetchColumn();
        
        $this->assertEquals('cancelled', $status, '订单状态应更新为已取消');
        
        // 验证取消消息是否已保存
        $stmt = $this->pdo->prepare("SELECT * FROM mq_messages WHERE topic = 'order_cancelled'");
        $stmt->execute();
        $messages = $stmt->fetchAll();
        
        $this->assertNotEmpty($messages, '应该有订单取消消息');
        $messageData = json_decode($messages[0]['data'], true);
        $this->assertEquals($orderId, $messageData['order_id'], '消息中的订单ID应匹配');
        
        // 使用消息验证方法再次确认
        $messageValid = $this->validateMessages('order_cancelled', function($data) use ($orderId) {
            return isset($data['order_id']) && $data['order_id'] == $orderId;
        });
        $this->assertTrue($messageValid, '订单取消消息验证应通过');
        
        $this->logger->info("取消订单测试成功", ['order_id' => $orderId]);
    }
    
    /**
     * 测试订单支付流程
     */
    public function testPayOrder()
    {
        // 先创建一个订单
        $orderData = [
            'user_id' => 1003,
            'total_amount' => 399.99,
            'items' => [
                [
                    'product_id' => 103,
                    'quantity' => 1,
                    'price' => 399.99
                ]
            ]
        ];
        
        $orderId = $this->orderService->createOrder($orderData);
        $this->assertNotEmpty($orderId, '订单创建应成功');
        
        // 分发创建订单的消息
        $this->dispatchPendingMessages();
        
        // 支付参数
        $paymentMethod = 'credit_card';
        $transactionId = 'txn_' . uniqid();
        
        // 执行支付操作
        $result = $this->orderService->payOrder($orderId, $paymentMethod, $transactionId);
        
        // 分发支付订单的消息
        $this->dispatchPendingMessages();
        
        // 验证支付结果
        $this->assertTrue($result, '支付订单应返回true');
        
        // 从数据库验证订单状态
        $stmt = $this->pdo->prepare("SELECT status FROM orders WHERE id = ?");
        $stmt->execute([$orderId]);
        $status = $stmt->fetchColumn();
        
        $this->assertEquals('paid', $status, '订单状态应更新为已支付');
        
        // 验证支付记录
        $stmt = $this->pdo->prepare("SELECT * FROM order_payments WHERE order_id = ?");
        $stmt->execute([$orderId]);
        $payment = $stmt->fetch();
        
        $this->assertNotEmpty($payment, '支付记录应存在');
        $this->assertEquals($paymentMethod, $payment['payment_method'], '支付方式应匹配');
        $this->assertEquals($transactionId, $payment['transaction_id'], '交易ID应匹配');
        $this->assertEquals($orderData['total_amount'], $payment['amount'], '支付金额应匹配');
        
        // 验证支付消息是否已保存
        $stmt = $this->pdo->prepare("SELECT * FROM mq_messages WHERE topic = 'order_paid'");
        $stmt->execute();
        $messages = $stmt->fetchAll();
        
        $this->assertNotEmpty($messages, '应该有订单支付消息');
        $messageData = json_decode($messages[0]['data'], true);
        $this->assertEquals($orderId, $messageData['order_id'], '消息中的订单ID应匹配');
        $this->assertEquals($paymentMethod, $messageData['payment_method'], '消息中的支付方式应匹配');
        
        // 使用消息验证方法再次确认
        $messageValid = $this->validateMessages('order_paid', function($data) use ($orderId, $paymentMethod) {
            return isset($data['order_id']) && $data['order_id'] == $orderId &&
                   isset($data['payment_method']) && $data['payment_method'] == $paymentMethod;
        });
        $this->assertTrue($messageValid, '订单支付消息验证应通过');
        
        $this->logger->info("支付订单测试成功", ['order_id' => $orderId, 'transaction_id' => $transactionId]);
    }
    
    /**
     * 测试幂等性 - 重复支付
     */
    public function testPayOrderIdempotence()
    {
        // 先创建一个订单
        $orderData = [
            'user_id' => 1004,
            'total_amount' => 499.99,
            'items' => [
                [
                    'product_id' => 104,
                    'quantity' => 1,
                    'price' => 499.99
                ]
            ]
        ];
        
        $orderId = $this->orderService->createOrder($orderData);
        $this->assertNotEmpty($orderId, '订单创建应成功');
        
        // 分发创建订单的消息
        $this->dispatchPendingMessages();
        
        // 支付参数
        $paymentMethod = 'alipay';
        $transactionId = 'txn_' . uniqid();
        
        // 第一次支付
        $result1 = $this->orderService->payOrder($orderId, $paymentMethod, $transactionId);
        $this->assertTrue($result1, '第一次支付应成功');
        
        // 分发第一次支付的消息
        $this->dispatchPendingMessages();
        
        // 第二次支付（应该是幂等的）
        $result2 = $this->orderService->payOrder($orderId, $paymentMethod, $transactionId . '_different');
        $this->assertTrue($result2, '重复支付应返回成功');
        
        // 分发第二次支付的消息
        $this->dispatchPendingMessages();
        
        // 验证支付记录数量（应该只有一条）
        $stmt = $this->pdo->prepare("SELECT COUNT(*) FROM order_payments WHERE order_id = ?");
        $stmt->execute([$orderId]);
        $count = $stmt->fetchColumn();
        
        $this->assertEquals(1, $count, '应该只有一条支付记录');
        
        $this->logger->info("支付幂等性测试成功", ['order_id' => $orderId]);
    }
    
    /**
     * 验证数据库中的消息是否正确并已准备好发送
     * 
     * @param string $topic 消息主题
     * @param callable $validator 消息验证回调函数
     * @param int|null $expectedCount 预期消息数量，为null时不检查数量
     * @return bool 验证结果
     */
    private function validateMessages(string $topic, callable $validator, ?int $expectedCount = null): bool
    {
        $stmt = $this->pdo->prepare("SELECT * FROM mq_messages WHERE topic = :topic AND status = 'pending'");
        $stmt->execute(['topic' => $topic]);
        $messages = $stmt->fetchAll();
        
        if (empty($messages)) {
            $this->logger->warning("没有找到待处理的 {$topic} 消息");
            return false;
        }
        
        // 如果指定了预期数量，验证消息数量
        if ($expectedCount !== null && count($messages) !== $expectedCount) {
            $this->logger->warning(
                "消息数量不匹配", 
                ['expected' => $expectedCount, 'actual' => count($messages), 'topic' => $topic]
            );
            return false;
        }
        
        $validCount = 0;
        foreach ($messages as $message) {
            $data = json_decode($message['data'], true);
            if ($validator($data)) {
                $validCount++;
                $this->logger->info("找到有效的 {$topic} 消息", [
                    'id' => $message['id'],
                    'data' => json_encode($data, JSON_UNESCAPED_UNICODE)
                ]);
            }
        }
        
        if ($expectedCount !== null && $validCount !== $expectedCount) {
            $this->logger->warning(
                "有效消息数量不匹配", 
                ['expected' => $expectedCount, 'actual' => $validCount, 'topic' => $topic]
            );
            return false;
        }
        
        return $validCount > 0;
    }
    
    /**
     * 测试直接数据库操作
     */
    public function testDirectDatabaseOperation()
    {
        $this->logger->info("===== 开始直接数据库操作测试 =====");
        
        // 1. 直接向orders表插入数据
        try {
            $this->pdo->exec("DELETE FROM order_items WHERE order_id = 9999");
            $this->pdo->exec("DELETE FROM orders WHERE id = 9999");
            
            $stmt = $this->pdo->prepare("INSERT INTO orders (id, user_id, total_amount, status, created_at) 
                                       VALUES (9999, 9999, 99.99, 'pending', NOW())");
            $result = $stmt->execute();
            
            $this->logger->info("直接插入订单", ['result' => $result ? 'success' : 'failed']);
            
            // 检查是否插入成功
            $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = 9999");
            $stmt->execute();
            $order = $stmt->fetch();
            
            $this->assertNotEmpty($order, '直接插入订单应该成功');
            $this->assertEquals(9999, $order['user_id'], '用户ID应该匹配');
            
            // 2. 向order_items表插入数据
            $stmt = $this->pdo->prepare("INSERT INTO order_items (order_id, product_id, quantity, price) 
                                       VALUES (9999, 9999, 1, 99.99)");
            $result = $stmt->execute();
            
            $this->logger->info("直接插入订单项", ['result' => $result ? 'success' : 'failed']);
            
            // 3. 向mq_messages表插入数据
            $messageId = uniqid('test-');
            $data = json_encode(['order_id' => 9999, 'user_id' => 9999]);
            
            $stmt = $this->pdo->prepare("INSERT INTO mq_messages 
                                       (message_id, topic, data, status, created_at, updated_at) 
                                       VALUES (:message_id, 'direct_test', :data, 'pending', NOW(), NOW())");
            $result = $stmt->execute([
                'message_id' => $messageId,
                'data' => $data
            ]);
            
            $this->logger->info("直接插入消息", ['result' => $result ? 'success' : 'failed', 'message_id' => $messageId]);
            
            // 检查是否插入成功
            $stmt = $this->pdo->prepare("SELECT * FROM mq_messages WHERE message_id = :message_id");
            $stmt->execute(['message_id' => $messageId]);
            $message = $stmt->fetch();
            
            $this->assertNotEmpty($message, '直接插入消息应该成功');
            $this->assertEquals('direct_test', $message['topic'], '主题应该匹配');
            
            $this->logger->info("===== 直接数据库操作测试成功 =====");
        } catch (\Exception $e) {
            $this->logger->error("直接数据库操作失败", ['error' => $e->getMessage(), 'trace' => $e->getTraceAsString()]);
            $this->fail('直接数据库操作应该成功: ' . $e->getMessage());
        }
    }
    
    /**
     * 专门测试事务行为
     */
    public function testTransactionBehavior()
    {
        $this->logger->info("===== 开始事务行为测试 =====");
        
        // 1. 正常场景：开始事务、保存数据、提交事务
        try {
            $this->logger->info("--- 测试正常事务流程 ---");
            $this->transactionHelper->beginTransaction();
            
            // 插入测试订单
            $stmt = $this->pdo->prepare("INSERT INTO orders (user_id, total_amount, status, created_at) 
                                       VALUES (:user_id, :total_amount, :status, :created_at)");
            $stmt->execute([
                'user_id' => 8001,
                'total_amount' => 88.88,
                'status' => 'pending',
                'created_at' => date('Y-m-d H:i:s')
            ]);
            $orderId = $this->pdo->lastInsertId();
            
            // 保存消息
            $messageId = 'test-txn-' . uniqid();
            $message = [
                'message_id' => $messageId,
                'topic' => 'test_transaction',
                'data' => ['order_id' => $orderId],
                'options' => [],
                'status' => 'pending',
                'created_at' => date('Y-m-d H:i:s'),
                'retry_count' => 0
            ];
            
            $this->transactionHelper->saveMessage($message);
            
            // 提交事务
            $this->transactionHelper->commit();
            
            // 验证数据已保存
            $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = :id");
            $stmt->execute(['id' => $orderId]);
            $order = $stmt->fetch();
            
            $stmt = $this->pdo->prepare("SELECT * FROM mq_messages WHERE message_id = :message_id");
            $stmt->execute(['message_id' => $messageId]);
            $savedMessage = $stmt->fetch();
            
            $this->assertNotEmpty($order, '正常事务应该成功保存订单');
            $this->assertNotEmpty($savedMessage, '正常事务应该成功保存消息');
            
            $this->logger->info("正常事务测试成功", [
                'order_id' => $orderId,
                'message_id' => $messageId
            ]);
        } catch (\Exception $e) {
            $this->logger->error("正常事务测试失败", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            $this->fail('正常事务测试应该成功: ' . $e->getMessage());
        }
        
        // 2. 回滚场景：开始事务、保存数据、回滚事务
        try {
            $this->logger->info("--- 测试事务回滚 ---");
            $this->transactionHelper->beginTransaction();
            
            // 插入测试订单
            $stmt = $this->pdo->prepare("INSERT INTO orders (user_id, total_amount, status, created_at) 
                                       VALUES (:user_id, :total_amount, :status, :created_at)");
            $stmt->execute([
                'user_id' => 8002,
                'total_amount' => 88.99,
                'status' => 'pending',
                'created_at' => date('Y-m-d H:i:s')
            ]);
            $orderId = $this->pdo->lastInsertId();
            
            // 保存消息
            $messageId = 'test-rollback-' . uniqid();
            $message = [
                'message_id' => $messageId,
                'topic' => 'test_rollback',
                'data' => ['order_id' => $orderId],
                'options' => [],
                'status' => 'pending',
                'created_at' => date('Y-m-d H:i:s'),
                'retry_count' => 0
            ];
            
            $this->transactionHelper->saveMessage($message);
            
            // 记录主键ID
            $stmt = $this->pdo->query("SELECT MAX(id) FROM orders");
            $maxOrderId = $stmt->fetchColumn();
            
            $stmt = $this->pdo->query("SELECT MAX(id) FROM mq_messages");
            $maxMessageId = $stmt->fetchColumn();
            
            // 回滚事务
            $this->transactionHelper->rollback();
            
            // 验证数据已回滚（不应该存在）
            $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = :id");
            $stmt->execute(['id' => $orderId]);
            $order = $stmt->fetch();
            
            $stmt = $this->pdo->prepare("SELECT * FROM mq_messages WHERE message_id = :message_id");
            $stmt->execute(['message_id' => $messageId]);
            $savedMessage = $stmt->fetch();
            
            $this->assertEmpty($order, '回滚事务后应该没有订单数据');
            $this->assertEmpty($savedMessage, '回滚事务后应该没有消息数据');
            
            // 验证自增主键是否增加
            $stmt = $this->pdo->query("SELECT MAX(id) FROM orders");
            $newMaxOrderId = $stmt->fetchColumn();
            
            $stmt = $this->pdo->query("SELECT MAX(id) FROM mq_messages");
            $newMaxMessageId = $stmt->fetchColumn();
            
            $this->logger->info("事务回滚后主键变化", [
                'orders.id' => ['before' => $maxOrderId, 'after' => $newMaxOrderId],
                'mq_messages.id' => ['before' => $maxMessageId, 'after' => $newMaxMessageId]
            ]);
            
            $this->logger->info("事务回滚测试成功");
        } catch (\Exception $e) {
            $this->logger->error("事务回滚测试失败", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            $this->fail('事务回滚测试应该成功: ' . $e->getMessage());
        }
        
        // 3. 测试嵌套事务，可能是问题所在
        try {
            $this->logger->info("--- 测试嵌套事务行为 ---");
            
            // 确保没有活跃事务
            if ($this->pdo->inTransaction()) {
                $this->logger->warning("测试开始前有未完成事务，尝试回滚");
                $this->pdo->rollBack();
            }
            
            // 检查PDO是否支持嵌套事务
            $this->pdo->beginTransaction();
            $nestedSupported = false;
            
            try {
                $nestedSupported = $this->pdo->beginTransaction();
                if ($nestedSupported) {
                    $this->pdo->commit();
                }
                $this->pdo->commit();
            } catch (\Exception $e) {
                if ($this->pdo->inTransaction()) {
                    $this->pdo->rollBack();
                }
                $nestedSupported = false;
                $this->logger->warning("PDO不支持嵌套事务", ['error' => $e->getMessage()]);
            }
            
            $this->logger->info("PDO嵌套事务支持", ['supported' => $nestedSupported ? 'yes' : 'no']);
            
            // 确保没有活跃事务
            if ($this->pdo->inTransaction()) {
                $this->logger->warning("仍有未完成事务，尝试回滚");
                $this->pdo->rollBack();
            }
            
            // 测试框架中的嵌套事务
            $beginResult = $this->transactionHelper->beginTransaction();
            $this->logger->info("外部事务已开始", ['result' => $beginResult ? 'success' : 'failed']);
            
            if (!$beginResult) {
                throw new \RuntimeException("无法开始外部事务");
            }
            
            // 插入外部事务数据
            $stmt = $this->pdo->prepare("INSERT INTO orders (user_id, total_amount, status, created_at) 
                                       VALUES (:user_id, :total_amount, :status, :created_at)");
            $insertResult = $stmt->execute([
                'user_id' => 8003,
                'total_amount' => 99.99,
                'status' => 'pending',
                'created_at' => date('Y-m-d H:i:s')
            ]);
            
            if (!$insertResult) {
                throw new \RuntimeException("插入外部订单失败: " . implode(', ', $stmt->errorInfo()));
            }
            
            $outerOrderId = $this->pdo->lastInsertId();
            $this->logger->info("外部事务已插入订单", ['order_id' => $outerOrderId]);
            
            $innerOrderId = null;
            $innerResult = false;
            
            try {
                // 尝试开始内部事务
                $innerResult = $this->transactionHelper->beginTransaction();
                $this->logger->info("内部事务已开始", ['result' => $innerResult ? 'success' : 'failed']);
                
                if (!$innerResult) {
                    throw new \RuntimeException("无法开始内部事务");
                }
                
                // 插入内部事务数据
                $stmt = $this->pdo->prepare("INSERT INTO orders (user_id, total_amount, status, created_at) 
                                           VALUES (:user_id, :total_amount, :status, :created_at)");
                $innerInsertResult = $stmt->execute([
                    'user_id' => 8004,
                    'total_amount' => 199.99,
                    'status' => 'pending',
                    'created_at' => date('Y-m-d H:i:s')
                ]);
                
                if (!$innerInsertResult) {
                    throw new \RuntimeException("插入内部订单失败: " . implode(', ', $stmt->errorInfo()));
                }
                
                $innerOrderId = $this->pdo->lastInsertId();
                $this->logger->info("内部事务已插入订单", ['order_id' => $innerOrderId]);
                
                // 验证内部事务数据是否可见
                $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = :id");
                $stmt->execute(['id' => $innerOrderId]);
                $innerOrderCheck = $stmt->fetch();
                $this->logger->info("内部订单是否可见", ['visible' => !empty($innerOrderCheck)]);
                
                // 有时候回滚内部事务，以测试嵌套回滚行为
                $shouldRollback = true;
                
                if ($shouldRollback) {
                    $rollbackResult = $this->transactionHelper->rollback();
                    $this->logger->info("内部事务已回滚", ['result' => $rollbackResult ? 'success' : 'failed']);
                    
                    // 检查内部事务数据在回滚后是否仍然可见
                    $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = :id");
                    $stmt->execute(['id' => $innerOrderId]);
                    $innerOrderAfterRollback = $stmt->fetch();
                    $this->logger->info("回滚后内部订单是否可见", ['visible' => !empty($innerOrderAfterRollback)]);
                    
                    // 检查外部事务是否仍在活跃
                    $isOtherTransactionActive = $this->pdo->inTransaction();
                    $this->logger->info("外部事务状态", ['active' => $isOtherTransactionActive ? 'yes' : 'no']);
                } else {
                    $commitResult = $this->transactionHelper->commit();
                    $this->logger->info("内部事务已提交", ['result' => $commitResult ? 'success' : 'failed']);
                }
            } catch (\Exception $e) {
                $this->logger->error("内部事务异常", [
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
                
                // 尝试回滚内部事务
                try {
                    $this->transactionHelper->rollback();
                } catch (\Exception $e2) {
                    $this->logger->error("回滚内部事务异常", ['error' => $e2->getMessage()]);
                }
            }
            
            // 检查外部事务状态
            $isTransactionActive = $this->pdo->inTransaction();
            $this->logger->info("准备提交外部事务", ['transaction_active' => $isTransactionActive ? 'yes' : 'no']);
            
            if (!$isTransactionActive) {
                $this->logger->error("外部事务已不存在，可能被内部回滚影响");
                // 不尝试提交
            } else {
                // 提交外部事务
                $commitResult = $this->transactionHelper->commit();
                $this->logger->info("外部事务已提交", ['result' => $commitResult ? 'success' : 'failed']);
            }
            
            // 验证数据状态
            $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = :id");
            $stmt->execute(['id' => $outerOrderId]);
            $outerOrder = $stmt->fetch();
            
            $innerOrder = null;
            if (isset($innerOrderId)) {
                $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = :id");
                $stmt->execute(['id' => $innerOrderId]);
                $innerOrder = $stmt->fetch();
            }
            
            $this->logger->info("嵌套事务测试结果", [
                'outer_order' => $outerOrder ? 'exists' : 'not_exists',
                'inner_order' => $innerOrder ? 'exists' : 'not_exists'
            ]);
            
        } catch (\Exception $e) {
            $this->logger->error("嵌套事务测试失败", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            
            // 确保任何活跃事务被回滚
            if ($this->pdo->inTransaction()) {
                try {
                    $this->pdo->rollBack();
                    $this->logger->info("活跃事务已回滚");
                } catch (\Exception $e2) {
                    $this->logger->error("回滚失败", ['error' => $e2->getMessage()]);
                }
            }
        }
        
        $this->logger->info("===== 事务行为测试完成 =====");
    }
    
    /**
     * 测试简单数据库连接和事务
     */
    public function testSimpleConnection()
    {
        $this->logger->info("===== 测试简单数据库连接和事务 =====");
        
        // 确保没有活跃事务
        if ($this->pdo->inTransaction()) {
            $this->pdo->rollBack();
        }
        
        try {
            // 1. 检查数据库连接
            $this->assertTrue($this->pdo instanceof \PDO, "PDO对象应该可用");
            
            // 2. 测试自动提交设置
            $currentAutoCommit = $this->pdo->getAttribute(\PDO::ATTR_AUTOCOMMIT);
            $this->logger->info("当前自动提交设置", ['autocommit' => $currentAutoCommit]);
            
            // 3. 禁用自动提交 - MySQL驱动可能返回0而不是false
            $this->pdo->setAttribute(\PDO::ATTR_AUTOCOMMIT, false);
            $newAutoCommit = $this->pdo->getAttribute(\PDO::ATTR_AUTOCOMMIT);
            $this->logger->info("设置后自动提交设置", ['autocommit' => $newAutoCommit]);
            
            // 不同的数据库驱动可能返回不同的值
            // MySQL驱动通常返回0而不是false，所以我们检查它是否为假值
            $this->assertTrue(empty($newAutoCommit), '应该可以禁用自动提交');
            
            // 4. 测试简单事务
            $this->pdo->beginTransaction();
            $this->assertTrue($this->pdo->inTransaction(), '应该可以开始事务');
            
            // 插入测试数据
            $stmt = $this->pdo->prepare("INSERT INTO orders (user_id, total_amount, status, created_at) 
                                        VALUES (7001, 77.77, 'pending', NOW())");
            $result = $stmt->execute();
            $this->assertTrue($result, '应该可以在事务中执行SQL');
            
            $orderId = $this->pdo->lastInsertId();
            $this->assertNotEmpty($orderId, '应该可以获取到自增ID');
            
            // 查询验证
            $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = ?");
            $stmt->execute([$orderId]);
            $order = $stmt->fetch();
            $this->assertNotEmpty($order, '应该可以在事务中查询数据');
            
            // 回滚事务
            $this->pdo->rollBack();
            $this->assertFalse($this->pdo->inTransaction(), '应该可以回滚事务');
            
            // 验证数据已回滚
            $stmt = $this->pdo->prepare("SELECT * FROM orders WHERE id = ?");
            $stmt->execute([$orderId]);
            $order = $stmt->fetch();
            $this->assertEmpty($order, '回滚后应该查不到数据');
            
            $this->logger->info("简单事务测试成功");
            
            // 重置设置 - 使用MySQL特定方式
            $this->pdo->exec("SET autocommit=1");
            $this->pdo->setAttribute(\PDO::ATTR_AUTOCOMMIT, true);
            $finalAutoCommit = $this->pdo->query("SELECT @@autocommit")->fetchColumn();
            $this->logger->info("重置后自动提交设置", ['autocommit' => $finalAutoCommit]);
        } catch (\Exception $e) {
            $this->logger->error("简单事务测试失败", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            
            throw $e;
        }
    }
} 