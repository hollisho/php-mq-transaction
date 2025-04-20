<?php

namespace hollisho\MQTransactionTests\Unit;

use hollisho\MQTransaction\Producer\TransactionProducer;
use hollisho\MQTransactionTests\Examples\OrderService\OrderService;
use PHPUnit\Framework\TestCase;
use PHPUnit\Framework\MockObject\MockObject;

/**
 * OrderService 单元测试
 */
class OrderServiceTest extends TestCase
{
    /**
     * @var \PDO|MockObject 数据库连接模拟对象
     */
    private $pdoMock;
    
    /**
     * @var TransactionProducer|MockObject 事务生产者模拟对象
     */
    private $producerMock;
    
    /**
     * @var OrderService 被测试的订单服务
     */
    private $orderService;
    
    /**
     * @var \PDOStatement|MockObject 语句模拟对象
     */
    private $stmtMock;
    
    /**
     * 测试前准备
     */
    protected function setUp(): void
    {
        // 创建模拟对象
        $this->pdoMock = $this->createMock(\PDO::class);
        $this->producerMock = $this->createMock(TransactionProducer::class);
        $this->stmtMock = $this->createMock(\PDOStatement::class);
        
        // 创建测试实例
        $this->orderService = new OrderService($this->pdoMock, $this->producerMock);
    }
    
    /**
     * 测试创建订单：成功场景
     */
    public function testCreateOrder_Success()
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
        
        $orderId = 12345;
        
        // 设置模拟行为
        $this->producerMock->expects($this->once())
            ->method('beginTransaction');
            
        // 模拟PDO准备语句
        $this->pdoMock->expects($this->exactly(2))
            ->method('prepare')
            ->willReturn($this->stmtMock);
            
        // 模拟语句执行
        $this->stmtMock->expects($this->exactly(2))
            ->method('execute')
            ->willReturn(true);
            
        // 模拟获取最后插入ID
        $this->pdoMock->expects($this->once())
            ->method('lastInsertId')
            ->willReturn((string)$orderId);
            
        // 模拟消息准备
        $this->producerMock->expects($this->once())
            ->method('prepareMessage')
            ->with(
                $this->equalTo('order_created'),
                $this->callback(function($message) use ($orderId, $orderData) {
                    // 验证关键字段存在
                    if (!isset($message['order_id']) || !isset($message['user_id']) || 
                        !isset($message['total_amount']) || !isset($message['items'])) {
                        return false;
                    }
                    
                    // 验证基本字段值
                    if ((string)$message['order_id'] !== (string)$orderId ||
                        $message['user_id'] != $orderData['user_id'] ||
                        $message['total_amount'] != $orderData['total_amount']) {
                        return false;
                    }
                    
                    // 验证items数组结构
                    if (count($message['items']) != count($orderData['items'])) {
                        return false;
                    }
                    
                    // 验证通过
                    return true;
                })
            );
            
        // 模拟事务提交
        $this->producerMock->expects($this->once())
            ->method('commit');
            
        // 执行测试
        $result = $this->orderService->createOrder($orderData);
        
        // 验证结果
        $this->assertEquals($orderId, $result, '应该返回正确的订单ID');
    }
    
    /**
     * 测试创建订单：失败场景
     */
    public function testCreateOrder_Failure()
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
        
        // 设置模拟行为
        $this->producerMock->expects($this->once())
            ->method('beginTransaction');
            
        // 模拟PDO准备语句但执行失败
        $this->pdoMock->expects($this->once())
            ->method('prepare')
            ->willReturn($this->stmtMock);
            
        // 模拟语句执行抛出异常
        $this->stmtMock->expects($this->once())
            ->method('execute')
            ->willThrowException(new \PDOException('Database error'));
            
        // 模拟事务回滚
        $this->producerMock->expects($this->once())
            ->method('rollback');
            
        // 模拟事务提交不会被调用
        $this->producerMock->expects($this->never())
            ->method('commit');
            
        // 期望抛出异常
        $this->expectException(\PDOException::class);
        
        // 执行测试
        $this->orderService->createOrder($orderData);
    }
    
    /**
     * 测试取消订单：成功场景
     */
    public function testCancelOrder_Success()
    {
        // 准备测试数据
        $orderId = 12345;
        $orderStatus = ['status' => 'pending'];
        
        // 设置模拟行为
        $this->producerMock->expects($this->once())
            ->method('beginTransaction');
            
        // 模拟PDO准备语句
        $this->pdoMock->expects($this->exactly(2))
            ->method('prepare')
            ->willReturn($this->stmtMock);
            
        // 模拟查询结果
        $this->stmtMock->expects($this->once())
            ->method('fetch')
            ->willReturn($orderStatus);
            
        // 模拟语句执行
        $this->stmtMock->expects($this->exactly(2))
            ->method('execute')
            ->willReturn(true);
            
        // 模拟消息准备
        $this->producerMock->expects($this->once())
            ->method('prepareMessage')
            ->with(
                $this->equalTo('order_cancelled'),
                $this->callback(function($message) use ($orderId) {
                    return $message['order_id'] === $orderId;
                })
            );
            
        // 模拟事务提交
        $this->producerMock->expects($this->once())
            ->method('commit');
            
        // 执行测试
        $result = $this->orderService->cancelOrder($orderId);
        
        // 验证结果
        $this->assertTrue($result, '应该返回true表示取消成功');
    }
    
    /**
     * 测试取消订单：订单不存在
     */
    public function testCancelOrder_OrderNotFound()
    {
        // 准备测试数据
        $orderId = 12345;
        
        // 设置模拟行为
        $this->producerMock->expects($this->once())
            ->method('beginTransaction');
            
        // 模拟PDO准备语句
        $this->pdoMock->expects($this->once())
            ->method('prepare')
            ->willReturn($this->stmtMock);
            
        // 模拟查询结果为空
        $this->stmtMock->expects($this->once())
            ->method('fetch')
            ->willReturn(false);
            
        // 模拟事务回滚
        $this->producerMock->expects($this->once())
            ->method('rollback');
            
        // 期望抛出异常
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage("Order not found: {$orderId}");
        
        // 执行测试
        $this->orderService->cancelOrder($orderId);
    }
    
    /**
     * 测试支付订单：成功场景
     */
    public function testPayOrder_Success()
    {
        // 准备测试数据
        $orderId = 12345;
        $paymentMethod = 'credit_card';
        $transactionId = 'txn_123456789';
        $orderData = [
            'status' => 'pending',
            'total_amount' => 199.99
        ];
        
        // 设置模拟行为
        $this->producerMock->expects($this->once())
            ->method('beginTransaction');
            
        // 模拟PDO准备语句
        $this->pdoMock->expects($this->exactly(3))
            ->method('prepare')
            ->willReturn($this->stmtMock);
            
        // 模拟查询结果
        $this->stmtMock->expects($this->once())
            ->method('fetch')
            ->willReturn($orderData);
            
        // 模拟语句执行
        $this->stmtMock->expects($this->exactly(3))
            ->method('execute')
            ->willReturn(true);
            
        // 模拟消息准备
        $this->producerMock->expects($this->once())
            ->method('prepareMessage')
            ->with(
                $this->equalTo('order_paid'),
                $this->callback(function($message) use ($orderId, $paymentMethod, $transactionId) {
                    return $message['order_id'] === $orderId &&
                           $message['payment_method'] === $paymentMethod &&
                           $message['transaction_id'] === $transactionId &&
                           $message['amount'] === 199.99;
                })
            );
            
        // 模拟事务提交
        $this->producerMock->expects($this->once())
            ->method('commit');
            
        // 执行测试
        $result = $this->orderService->payOrder($orderId, $paymentMethod, $transactionId);
        
        // 验证结果
        $this->assertTrue($result, '应该返回true表示支付成功');
    }
    
    /**
     * 测试支付订单：订单已支付
     */
    public function testPayOrder_AlreadyPaid()
    {
        // 准备测试数据
        $orderId = 12345;
        $paymentMethod = 'credit_card';
        $transactionId = 'txn_123456789';
        $orderData = [
            'status' => 'paid',
            'total_amount' => 199.99
        ];
        
        // 设置模拟行为
        $this->producerMock->expects($this->once())
            ->method('beginTransaction');
            
        // 模拟PDO准备语句
        $this->pdoMock->expects($this->once())
            ->method('prepare')
            ->willReturn($this->stmtMock);
            
        // 模拟查询结果
        $this->stmtMock->expects($this->once())
            ->method('fetch')
            ->willReturn($orderData);
            
        // 模拟事务回滚
        $this->producerMock->expects($this->once())
            ->method('rollback');
            
        // 模拟事务提交不会被调用
        $this->producerMock->expects($this->never())
            ->method('commit');
            
        // 执行测试
        $result = $this->orderService->payOrder($orderId, $paymentMethod, $transactionId);
        
        // 验证结果
        $this->assertTrue($result, '对于已支付的订单应该返回true');
    }
}