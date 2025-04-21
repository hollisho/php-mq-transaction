<?php

namespace hollisho\MQTransactionTests\Examples\InventoryService;

use hollisho\MQTransaction\Consumer\EventConsumer;
use hollisho\MQTransaction\Database\IdempotencyHelper;
use Psr\Log\LoggerInterface;

/**
 * 库存服务示例
 */
class InventoryService
{
    /**
     * @var \PDO 数据库连接
     */
    private $pdo;
    
    /**
     * @var EventConsumer 消息消费者
     */
    private $consumer;
    
    /**
     * @var LoggerInterface 日志记录器
     */
    private $logger;
    
    /**
     * 构造函数
     * 
     * @param \PDO $pdo 数据库连接
     * @param EventConsumer $consumer 消息消费者
     * @param LoggerInterface|null $logger 日志记录器
     */
    public function __construct(\PDO $pdo, EventConsumer $consumer, ?LoggerInterface $logger = null)
    {
        $this->pdo = $pdo;
        $this->consumer = $consumer;
        $this->logger = $logger;
    }
    
    /**
     * 初始化库存服务
     * 
     * @return bool 是否初始化成功
     */
    public function initialize(): bool
    {
        try {
            // 创建库存表（如果不存在）
            $this->createInventoryTable();
            
            // 注册订单创建消息处理器
            $this->registerMessageHandlers();
            
            return true;
        } catch (\Exception $e) {
            if ($this->logger) {
                $this->logger->error("初始化库存服务失败", ['error' => $e->getMessage()]);
            }
            return false;
        }
    }
    
    /**
     * 创建库存表
     */
    public function createInventoryTable(): void
    {
        $this->pdo->exec("CREATE TABLE IF NOT EXISTS inventory (
            id INT AUTO_INCREMENT PRIMARY KEY,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            reserved INT NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL,
            updated_at DATETIME DEFAULT NULL,
            UNIQUE KEY (product_id)
        )");
        
        $this->pdo->exec("CREATE TABLE IF NOT EXISTS inventory_transactions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            type ENUM('reserve', 'commit', 'rollback') NOT NULL,
            created_at DATETIME NOT NULL,
            KEY (order_id),
            KEY (product_id)
        )");
    }
    
    /**
     * 注册消息处理器
     */
    private function registerMessageHandlers(): void
    {
        // 注册订单创建消息处理器
        $this->consumer->registerHandler('order_created', function ($data) {
            if ($this->logger) {
                $this->logger->info("收到订单创建消息", ['order_id' => $data['order_id']]);
            }
            
            return $this->handleOrderCreated($data);
        });
        
        // 注册订单支付消息处理器
        $this->consumer->registerHandler('order_paid', function ($data) {
            if ($this->logger) {
                $this->logger->info("收到订单支付消息", ['order_id' => $data['order_id']]);
            }
            
            return $this->handleOrderPaid($data);
        });
        
        // 注册订单取消消息处理器
        $this->consumer->registerHandler('order_cancelled', function ($data) {
            if ($this->logger) {
                $this->logger->info("收到订单取消消息", ['order_id' => $data['order_id']]);
            }
            
            return $this->cancelOrder($data['order_id']);
        });
    }
    
    /**
     * 处理订单创建消息，扣减库存
     * 
     * @param array $data 订单数据
     * @return bool 处理结果
     */
    public function handleOrderCreated(array $data): bool
    {
        if (!isset($data['order_id']) || !isset($data['items']) || !is_array($data['items'])) {
            if ($this->logger) {
                $this->logger->error("订单数据格式不正确", ['data' => json_encode($data)]);
            }
            return false;
        }
        
        $orderId = $data['order_id'];
        
        // 检查是否已经处理过该订单（幂等性检查）
        $stmt = $this->pdo->prepare("SELECT COUNT(*) FROM inventory_transactions WHERE order_id = :order_id AND type = 'reserve'");
        $stmt->execute(['order_id' => $orderId]);
        $count = (int)$stmt->fetchColumn();
        
        if ($count > 0) {
            if ($this->logger) {
                $this->logger->info("订单已处理过，跳过", ['order_id' => $orderId]);
            }
            return true; // 已处理过，视为成功
        }
        
        try {
            // 开始事务
            $this->pdo->beginTransaction();
            
            // 处理订单中的每个商品
            foreach ($data['items'] as $item) {
                $productId = $item['product_id'];
                $quantity = $item['quantity'];
                
                // 检查库存是否充足
                $stmt = $this->pdo->prepare("SELECT id, quantity, reserved FROM inventory WHERE product_id = :product_id FOR UPDATE");
                $stmt->execute(['product_id' => $productId]);
                $inventory = $stmt->fetch(\PDO::FETCH_ASSOC);
                
                if (!$inventory) {
                    throw new \RuntimeException("商品库存记录不存在: {$productId}");
                }
                
                $availableQuantity = $inventory['quantity'] - $inventory['reserved'];
                
                if ($availableQuantity < $quantity) {
                    throw new \RuntimeException("商品库存不足: {$productId}，需要: {$quantity}，可用: {$availableQuantity}");
                }
                
                // 更新库存：增加预留数量
                $stmt = $this->pdo->prepare("UPDATE inventory SET reserved = reserved + :quantity, updated_at = NOW() WHERE id = :id");
                $stmt->execute([
                    'id' => $inventory['id'],
                    'quantity' => $quantity
                ]);
                
                // 记录库存交易
                $stmt = $this->pdo->prepare("INSERT INTO inventory_transactions (order_id, product_id, quantity, type, created_at) VALUES (:order_id, :product_id, :quantity, 'reserve', NOW())");
                $stmt->execute([
                    'order_id' => $orderId,
                    'product_id' => $productId,
                    'quantity' => $quantity
                ]);
                
                if ($this->logger) {
                    $this->logger->info("商品库存已预留", [
                        'order_id' => $orderId,
                        'product_id' => $productId,
                        'quantity' => $quantity
                    ]);
                }
            }
            
            // 提交事务
            $this->pdo->commit();
            
            if ($this->logger) {
                $this->logger->info("订单库存处理成功", ['order_id' => $orderId]);
            }
            
            return true;
        } catch (\Exception $e) {
            // 回滚事务
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            
            if ($this->logger) {
                $this->logger->error("订单库存处理失败", [
                    'order_id' => $orderId,
                    'error' => $e->getMessage()
                ]);
            }
            
            return false;
        }
    }
    
    /**
     * 新增商品库存
     * 
     * @param int $productId 商品ID
     * @param int $quantity 数量
     * @return bool 操作结果
     */
    public function addProductStock(int $productId, int $quantity): bool
    {
        try {
            // 开始事务
            $this->pdo->beginTransaction();
            
            // 检查商品是否已有库存记录 - 使用较低的锁级别
            $stmt = $this->pdo->prepare("SELECT id FROM inventory WHERE product_id = :product_id");
            $stmt->execute(['product_id' => $productId]);
            $inventory = $stmt->fetch(\PDO::FETCH_ASSOC);
            
            if ($inventory) {
                // 更新现有库存记录
                $stmt = $this->pdo->prepare("UPDATE inventory SET quantity = quantity + :quantity, updated_at = NOW() WHERE id = :id");
                $result = $stmt->execute([
                    'id' => $inventory['id'],
                    'quantity' => $quantity
                ]);
            } else {
                // 创建新的库存记录
                $stmt = $this->pdo->prepare("INSERT INTO inventory (product_id, quantity, reserved, created_at) VALUES (:product_id, :quantity, 0, NOW())");
                $result = $stmt->execute([
                    'product_id' => $productId,
                    'quantity' => $quantity
                ]);
            }
            
            // 提交事务
            $this->pdo->commit();
            
            if ($this->logger) {
                $this->logger->info("商品库存已增加", [
                    'product_id' => $productId,
                    'quantity' => $quantity
                ]);
            }
            
            return $result;
        } catch (\Exception $e) {
            // 回滚事务
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            
            if ($this->logger) {
                $this->logger->error("增加商品库存失败", [
                    'product_id' => $productId,
                    'error' => $e->getMessage()
                ]);
            }
            
            return false;
        }
    }
    
    /**
     * 确认订单，将预留库存转为已使用
     * 
     * @param int $orderId 订单ID
     * @return bool 操作结果
     */
    public function confirmOrder(int $orderId): bool
    {
        try {
            // 检查是否已经确认过该订单（幂等性检查）
            $stmt = $this->pdo->prepare("SELECT COUNT(*) FROM inventory_transactions WHERE order_id = :order_id AND type = 'commit'");
            $stmt->execute(['order_id' => $orderId]);
            $count = (int)$stmt->fetchColumn();
            
            if ($count > 0) {
                if ($this->logger) {
                    $this->logger->info("订单已确认过，跳过", ['order_id' => $orderId]);
                }
                return true; // 已处理过，视为成功
            }
            
            $this->pdo->beginTransaction();
            
            // 查找此订单预留的库存
            $stmt = $this->pdo->prepare("SELECT product_id, SUM(quantity) as total_quantity FROM inventory_transactions WHERE order_id = :order_id AND type = 'reserve' GROUP BY product_id");
            $stmt->execute(['order_id' => $orderId]);
            $reservations = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            
            if (empty($reservations)) {
                if ($this->logger) {
                    $this->logger->warning("未找到订单的库存预留记录", ['order_id' => $orderId]);
                }
                $this->pdo->rollBack();
                return false;
            }
            
            foreach ($reservations as $reservation) {
                $productId = $reservation['product_id'];
                $quantity = $reservation['total_quantity'];
                
                // 更新库存：减少实际库存和预留库存
                // 注意：这里只需要减少预留库存，因为实际库存在预留时并未减少
                $stmt = $this->pdo->prepare("UPDATE inventory SET quantity = quantity - :quantity, reserved = reserved - :quantity, updated_at = NOW() WHERE product_id = :product_id");
                $stmt->execute([
                    'product_id' => $productId,
                    'quantity' => $quantity
                ]);
                
                // 记录库存交易
                $stmt = $this->pdo->prepare("INSERT INTO inventory_transactions (order_id, product_id, quantity, type, created_at) VALUES (:order_id, :product_id, :quantity, 'commit', NOW())");
                $stmt->execute([
                    'order_id' => $orderId,
                    'product_id' => $productId,
                    'quantity' => $quantity
                ]);
                
                if ($this->logger) {
                    $this->logger->info("订单商品库存已确认扣减", [
                        'order_id' => $orderId,
                        'product_id' => $productId,
                        'quantity' => $quantity
                    ]);
                }
            }
            
            $this->pdo->commit();
            
            if ($this->logger) {
                $this->logger->info("订单库存确认成功", ['order_id' => $orderId]);
            }
            
            return true;
        } catch (\Exception $e) {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            
            if ($this->logger) {
                $this->logger->error("确认订单库存失败", [
                    'order_id' => $orderId,
                    'error' => $e->getMessage()
                ]);
            }
            
            return false;
        }
    }
    
    /**
     * 取消订单，释放预留库存
     * 
     * @param int $orderId 订单ID
     * @return bool 操作结果
     */
    public function cancelOrder(int $orderId): bool
    {
        try {
            $this->pdo->beginTransaction();
            
            // 查找此订单预留的库存
            $stmt = $this->pdo->prepare("SELECT product_id, SUM(quantity) as total_quantity FROM inventory_transactions WHERE order_id = :order_id AND type = 'reserve' GROUP BY product_id");
            $stmt->execute(['order_id' => $orderId]);
            $reservations = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            
            if (empty($reservations)) {
                if ($this->logger) {
                    $this->logger->warning("未找到订单的库存预留记录", ['order_id' => $orderId]);
                }
                $this->pdo->rollBack();
                return false;
            }
            
            foreach ($reservations as $reservation) {
                $productId = $reservation['product_id'];
                $quantity = $reservation['total_quantity'];
                
                // 更新库存：减少预留数量
                $stmt = $this->pdo->prepare("UPDATE inventory SET reserved = reserved - :quantity, updated_at = NOW() WHERE product_id = :product_id");
                $stmt->execute([
                    'product_id' => $productId,
                    'quantity' => $quantity
                ]);
                
                // 记录库存交易
                $stmt = $this->pdo->prepare("INSERT INTO inventory_transactions (order_id, product_id, quantity, type, created_at) VALUES (:order_id, :product_id, :quantity, 'rollback', NOW())");
                $stmt->execute([
                    'order_id' => $orderId,
                    'product_id' => $productId,
                    'quantity' => $quantity
                ]);
                
                if ($this->logger) {
                    $this->logger->info("订单商品库存已释放", [
                        'order_id' => $orderId,
                        'product_id' => $productId,
                        'quantity' => $quantity
                    ]);
                }
            }
            
            $this->pdo->commit();
            
            if ($this->logger) {
                $this->logger->info("订单库存取消成功", ['order_id' => $orderId]);
            }
            
            return true;
        } catch (\Exception $e) {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            
            if ($this->logger) {
                $this->logger->error("取消订单库存失败", [
                    'order_id' => $orderId,
                    'error' => $e->getMessage()
                ]);
            }
            
            return false;
        }
    }
    
    /**
     * 处理订单支付消息，确认库存扣减
     * 
     * @param array $data 支付数据
     * @return bool 处理结果
     */
    public function handleOrderPaid(array $data): bool
    {
        if (!isset($data['order_id'])) {
            if ($this->logger) {
                $this->logger->error("支付数据格式不正确", ['data' => json_encode($data)]);
            }
            return false;
        }
        
        $orderId = $data['order_id'];
        
        try {
            // 调用确认订单方法，将预留库存转为已使用
            $result = $this->confirmOrder($orderId);
            
            if ($result) {
                if ($this->logger) {
                    $this->logger->info("订单支付后库存已确认扣减", ['order_id' => $orderId]);
                }
            } else {
                if ($this->logger) {
                    $this->logger->warning("订单支付后库存确认失败", ['order_id' => $orderId]);
                }
            }
            
            return $result;
        } catch (\Exception $e) {
            if ($this->logger) {
                $this->logger->error("处理订单支付消息失败", [
                    'order_id' => $orderId,
                    'error' => $e->getMessage()
                ]);
            }
            
            return false;
        }
    }
    
    /**
     * 启动消费者服务
     * 
     * @param array $topics 要消费的主题列表
     * @param int $maxMessages 最大消费消息数，默认为0表示持续消费
     * @return bool 操作结果
     */
    public function startConsuming(array $topics = ['order_created'], int $maxMessages = 0): bool
    {
        try {
            if ($this->logger) {
                $this->logger->info("启动库存服务消费者", ['topics' => $topics]);
            }
            
            $this->consumer->startConsuming($topics, $maxMessages);
            return true;
        } catch (\Exception $e) {
            if ($this->logger) {
                $this->logger->error("启动库存服务消费者失败", [
                    'error' => $e->getMessage()
                ]);
            }
            
            return false;
        }
    }
    
    /**
     * 获取商品库存
     * 
     * @param int $productId 商品ID
     * @return array|null 库存信息
     */
    public function getProductStock(int $productId): ?array
    {
        try {
            $stmt = $this->pdo->prepare("SELECT * FROM inventory WHERE product_id = :product_id");
            $stmt->execute(['product_id' => $productId]);
            $inventory = $stmt->fetch(\PDO::FETCH_ASSOC);
            
            if ($inventory) {
                $inventory['available'] = $inventory['quantity'] - $inventory['reserved'];
            }
            
            return $inventory ?: null;
        } catch (\Exception $e) {
            if ($this->logger) {
                $this->logger->error("获取商品库存失败", [
                    'product_id' => $productId,
                    'error' => $e->getMessage()
                ]);
            }
            
            return null;
        }
    }
}