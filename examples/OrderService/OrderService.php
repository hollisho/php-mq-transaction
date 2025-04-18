<?php

namespace hollisho\MQTransaction\Examples\OrderService;

use hollisho\MQTransaction\Producer\TransactionProducer;

/**
 * 订单服务示例
 */
class OrderService
{
    /**
     * @var \PDO 数据库连接
     */
    private $pdo;
    
    /**
     * @var TransactionProducer 事务性消息生产者
     */
    private $producer;
    
    /**
     * 构造函数
     * 
     * @param \PDO $pdo 数据库连接
     * @param TransactionProducer $producer 事务性消息生产者
     */
    public function __construct(\PDO $pdo, TransactionProducer $producer)
    {
        $this->pdo = $pdo;
        $this->producer = $producer;
    }
    
    /**
     * 创建订单
     * 
     * @param array $orderData 订单数据
     * @return int 订单ID
     * @throws \Exception 创建失败时抛出异常
     */
    public function createOrder(array $orderData): int
    {
        // 开始事务
        $this->producer->beginTransaction();
        
        try {
            // 1. 保存订单基本信息
            $stmt = $this->pdo->prepare("INSERT INTO orders (user_id, total_amount, status, created_at) VALUES (:user_id, :total_amount, 'pending', :created_at)");
            $stmt->execute([
                'user_id' => $orderData['user_id'],
                'total_amount' => $orderData['total_amount'],
                'created_at' => date('Y-m-d H:i:s')
            ]);
            
            $orderId = $this->pdo->lastInsertId();
            
            // 2. 保存订单项
            foreach ($orderData['items'] as $item) {
                $stmt = $this->pdo->prepare("INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (:order_id, :product_id, :quantity, :price)");
                $stmt->execute([
                    'order_id' => $orderId,
                    'product_id' => $item['product_id'],
                    'quantity' => $item['quantity'],
                    'price' => $item['price']
                ]);
            }
            
            // 3. 准备发送订单创建消息
            $this->producer->prepareMessage('order_created', [
                'order_id' => $orderId,
                'user_id' => $orderData['user_id'],
                'total_amount' => $orderData['total_amount'],
                'items' => $orderData['items']
            ]);
            
            // 4. 提交事务（本地事务和消息发送作为一个原子操作）
            $this->producer->commit();
            
            return $orderId;
        } catch (\Exception $e) {
            // 回滚事务
            $this->producer->rollback();
            throw $e;
        }
    }
    
    /**
     * 取消订单
     * 
     * @param int $orderId 订单ID
     * @return bool 操作结果
     * @throws \Exception 取消失败时抛出异常
     */
    public function cancelOrder(int $orderId): bool
    {
        // 开始事务
        $this->producer->beginTransaction();
        
        try {
            // 1. 检查订单状态
            $stmt = $this->pdo->prepare("SELECT status FROM orders WHERE id = :id");
            $stmt->execute(['id' => $orderId]);
            $order = $stmt->fetch(\PDO::FETCH_ASSOC);
            
            if (!$order) {
                throw new \RuntimeException("Order not found: {$orderId}");
            }
            
            if ($order['status'] === 'cancelled') {
                // 已经取消，直接返回成功
                $this->producer->rollback();
                return true;
            }
            
            if ($order['status'] !== 'pending') {
                throw new \RuntimeException("Cannot cancel order with status: {$order['status']}");
            }
            
            // 2. 更新订单状态为已取消
            $stmt = $this->pdo->prepare("UPDATE orders SET status = 'cancelled', updated_at = :updated_at WHERE id = :id");
            $stmt->execute([
                'id' => $orderId,
                'updated_at' => date('Y-m-d H:i:s')
            ]);
            
            // 3. 准备发送订单取消消息
            $this->producer->prepareMessage('order_cancelled', [
                'order_id' => $orderId
            ]);
            
            // 4. 提交事务
            $this->producer->commit();
            
            return true;
        } catch (\Exception $e) {
            // 回滚事务
            $this->producer->rollback();
            throw $e;
        }
    }
    
    /**
     * 支付订单
     * 
     * @param int $orderId 订单ID
     * @param string $paymentMethod 支付方式
     * @param string $transactionId 支付交易ID
     * @return bool 操作结果
     * @throws \Exception 支付失败时抛出异常
     */
    public function payOrder(int $orderId, string $paymentMethod, string $transactionId): bool
    {
        // 开始事务
        $this->producer->beginTransaction();
        
        try {
            // 1. 检查订单状态
            $stmt = $this->pdo->prepare("SELECT status, total_amount FROM orders WHERE id = :id");
            $stmt->execute(['id' => $orderId]);
            $order = $stmt->fetch(\PDO::FETCH_ASSOC);
            
            if (!$order) {
                throw new \RuntimeException("Order not found: {$orderId}");
            }
            
            if ($order['status'] === 'paid') {
                // 已经支付，直接返回成功
                $this->producer->rollback();
                return true;
            }
            
            if ($order['status'] !== 'pending') {
                throw new \RuntimeException("Cannot pay order with status: {$order['status']}");
            }
            
            // 2. 记录支付信息
            $stmt = $this->pdo->prepare("INSERT INTO order_payments (order_id, payment_method, transaction_id, amount, status, created_at) VALUES (:order_id, :payment_method, :transaction_id, :amount, 'success', :created_at)");
            $stmt->execute([
                'order_id' => $orderId,
                'payment_method' => $paymentMethod,
                'transaction_id' => $transactionId,
                'amount' => $order['total_amount'],
                'created_at' => date('Y-m-d H:i:s')
            ]);
            
            // 3. 更新订单状态为已支付
            $stmt = $this->pdo->prepare("UPDATE orders SET status = 'paid', updated_at = :updated_at WHERE id = :id");
            $stmt->execute([
                'id' => $orderId,
                'updated_at' => date('Y-m-d H:i:s')
            ]);
            
            // 4. 准备发送订单支付消息
            $this->producer->prepareMessage('order_paid', [
                'order_id' => $orderId,
                'payment_method' => $paymentMethod,
                'transaction_id' => $transactionId,
                'amount' => $order['total_amount']
            ]);
            
            // 5. 提交事务
            $this->producer->commit();
            
            return true;
        } catch (\Exception $e) {
            // 回滚事务
            $this->producer->rollback();
            throw $e;
        }
    }

}